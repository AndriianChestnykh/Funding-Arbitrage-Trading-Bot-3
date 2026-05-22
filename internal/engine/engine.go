// Package engine computes funding-rate arbitrage opportunities from a stream of ticks.
package engine

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/shopspring/decimal"

	"github.com/AndriianChestnykh/Funding-Arbitrage-Trading-Bot-3/internal/domain"
)

// Config holds the engine's tuning parameters.
type Config struct {
	MinNetEdge          decimal.Decimal // minimum net edge per funding period
	CostPerRoundTrip    decimal.Decimal // estimated total round-trip cost
	PreSettlementWindow time.Duration   // ignore opportunities this close to settlement
	DebounceInterval    time.Duration   // min interval between candidates for the same pair
}

type pairKey struct{ short, long, symbol string }

// Engine fans in ticks from all venues, maintains a per-symbol snapshot,
// and emits Candidates when the net edge exceeds the threshold.
type Engine struct {
	cfg     Config
	mu      sync.RWMutex
	latest  map[string]map[string]domain.Tick // venue → symbol → tick
	lastEmit map[pairKey]time.Time
}

// New creates an Engine.
func New(cfg Config) *Engine {
	if cfg.DebounceInterval == 0 {
		cfg.DebounceInterval = time.Second
	}
	return &Engine{
		cfg:      cfg,
		latest:   make(map[string]map[string]domain.Tick),
		lastEmit: make(map[pairKey]time.Time),
	}
}

// Run reads from tickCh and writes Candidates to candidateCh until ctx is done.
// This function blocks and should be run in its own goroutine.
func (e *Engine) Run(ctx context.Context, tickCh <-chan domain.Tick, candidateCh chan<- domain.Candidate) {
	slog.Info("engine: started")
	for {
		select {
		case <-ctx.Done():
			slog.Info("engine: shutting down")
			return
		case tick, ok := <-tickCh:
			if !ok {
				return
			}
			e.update(tick)
			if candidates := e.evaluate(tick.Symbol); len(candidates) > 0 {
				for _, c := range candidates {
					select {
					case candidateCh <- c:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}
}

func (e *Engine) update(tick domain.Tick) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.latest[tick.Venue] == nil {
		e.latest[tick.Venue] = make(map[string]domain.Tick)
	}
	e.latest[tick.Venue][tick.Symbol] = tick
}

// evaluate checks all venue pairs for the given symbol and returns candidates.
func (e *Engine) evaluate(symbol string) []domain.Candidate {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Collect all venues that have a fresh tick for this symbol.
	type entry struct {
		venue string
		tick  domain.Tick
	}
	var entries []entry
	for venue, bySymbol := range e.latest {
		if tick, ok := bySymbol[symbol]; ok {
			if time.Since(tick.Received) < 60*time.Second { // staleness guard
				entries = append(entries, entry{venue, tick})
			}
		}
	}

	if len(entries) < 2 {
		return nil
	}

	var candidates []domain.Candidate
	now := time.Now()

	for i := 0; i < len(entries); i++ {
		for j := i + 1; j < len(entries); j++ {
			a, b := entries[i], entries[j]

			// Determine which venue to short and which to long.
			// Short the venue with the higher rate (longs pay more there).
			// Long the venue with the lower rate.
			short, long := a, b
			if b.tick.Rate.GreaterThan(a.tick.Rate) {
				short, long = b, a
			}

			// Net edge = (shortRate - longRate) - roundTripCost
			spread := short.tick.Rate.Sub(long.tick.Rate)
			netEdge := spread.Sub(e.cfg.CostPerRoundTrip)

			if netEdge.LessThanOrEqual(e.cfg.MinNetEdge) {
				continue
			}

			// TTF: use the shorter of the two (more conservative).
			ttfShort := short.tick.TimeToFunding()
			ttfLong := long.tick.TimeToFunding()
			ttf := ttfShort
			if ttfLong < ttf {
				ttf = ttfLong
			}

			// Skip if too close to settlement.
			if ttf < e.cfg.PreSettlementWindow {
				slog.Debug("engine: skipping — too close to settlement",
					"symbol", symbol, "ttf", ttf)
				continue
			}

			// Debounce: don't spam the same pair.
			key := pairKey{short.venue, long.venue, symbol}
			if last, ok := e.lastEmit[key]; ok && now.Sub(last) < e.cfg.DebounceInterval {
				continue
			}
			e.lastEmit[key] = now

			candidates = append(candidates, domain.Candidate{
				ShortVenue: short.venue,
				LongVenue:  long.venue,
				Symbol:     symbol,
				ShortRate:  short.tick.Rate,
				LongRate:   long.tick.Rate,
				NetEdge:    netEdge,
				TTF:        ttf,
			})

			slog.Info("engine: candidate found",
				"symbol", symbol,
				"short_venue", short.venue,
				"long_venue", long.venue,
				"net_edge_bps", netEdge.Mul(decimal.NewFromInt(10000)).StringFixed(2),
				"ttf", ttf.Round(time.Minute))
		}
	}
	return candidates
}
