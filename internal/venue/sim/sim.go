// Package sim provides a simulated venue for paper trading and testing.
// It generates synthetic funding ticks with realistic rate distributions.
package sim

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/shopspring/decimal"

	"github.com/AndriianChestnykh/Funding-Arbitrage-Trading-Bot-3/internal/domain"
)

const tickInterval = 5 * time.Second

// Venue simulates a perpetual futures exchange.
type Venue struct {
	name string
	// baseRates holds a fixed "bias" per symbol so different sim venues diverge.
	baseRateBps float64
}

// New creates a simulated venue. baseRateBps sets the mean funding rate bias
// (positive → bullish skew, negative → bearish skew).
func New(name string, baseRateBps float64) *Venue {
	return &Venue{name: name, baseRateBps: baseRateBps}
}

func (v *Venue) Name() string { return v.name }

func (v *Venue) SubscribeFunding(ctx context.Context, symbols []string) (<-chan domain.Tick, error) {
	ch := make(chan domain.Tick, 64)
	go func() {
		defer close(ch)
		ticker := time.NewTicker(tickInterval)
		defer ticker.Stop()

		// Simulate funding settling every 8h; stagger next settlement.
		nextFunding := time.Now().Add(8 * time.Hour).Truncate(8 * time.Hour)

		for {
			select {
			case <-ctx.Done():
				return
			case t := <-ticker.C:
				for _, sym := range symbols {
					rate := syntheticRate(sym, v.baseRateBps)
					mark := syntheticMarkPrice(sym)
					tick := domain.Tick{
						Venue:     v.name,
						Symbol:    sym,
						Rate:      rate,
						NextAt:    nextFunding,
						MarkPrice: mark,
						Received:  t,
					}
					select {
					case ch <- tick:
					default:
						slog.Warn("sim: tick channel full, dropping", "venue", v.name, "symbol", sym)
					}
				}
				// Advance funding window once past.
				if time.Now().After(nextFunding) {
					nextFunding = nextFunding.Add(8 * time.Hour)
				}
			}
		}
	}()
	return ch, nil
}

func (v *Venue) PlaceOrder(_ context.Context, o domain.Order) (domain.Fill, error) {
	slog.Info("sim: placing order (paper)",
		"venue", v.name, "symbol", o.Symbol, "side", o.Side, "size", o.Size)
	fill := domain.Fill{
		OrderID:  fmt.Sprintf("sim-%d", time.Now().UnixNano()),
		Venue:    v.name,
		Symbol:   o.Symbol,
		Side:     o.Side,
		Size:     o.Size,
		Price:    syntheticMarkPrice(o.Symbol),
		Fee:      o.Size.Mul(syntheticMarkPrice(o.Symbol)).Mul(decimal.NewFromFloat(0.0005)),
		FilledAt: time.Now(),
	}
	return fill, nil
}

func (v *Venue) ClosePosition(_ context.Context, symbol string, side domain.Side, size decimal.Decimal) (domain.Fill, error) {
	slog.Info("sim: closing position (paper)", "venue", v.name, "symbol", symbol, "side", side, "size", size)
	closeSide := domain.SideLong
	if side == domain.SideLong {
		closeSide = domain.SideShort
	}
	fill := domain.Fill{
		OrderID:  fmt.Sprintf("sim-close-%d", time.Now().UnixNano()),
		Venue:    v.name,
		Symbol:   symbol,
		Side:     closeSide,
		Size:     size,
		Price:    syntheticMarkPrice(symbol),
		Fee:      size.Mul(syntheticMarkPrice(symbol)).Mul(decimal.NewFromFloat(0.0005)),
		FilledAt: time.Now(),
	}
	return fill, nil
}

// syntheticRate generates a plausible funding rate for a symbol.
// Real rates cluster around ±0.01% with occasional spikes.
func syntheticRate(symbol string, biasRateBps float64) decimal.Decimal {
	noise := (rand.Float64() - 0.5) * 0.001 // ±0.05% noise
	bias := biasRateBps / 10000.0
	_ = symbol // future: symbol-specific bias
	return decimal.NewFromFloat(bias + noise)
}

// syntheticMarkPrice returns a plausible USD mark price per symbol.
func syntheticMarkPrice(symbol string) decimal.Decimal {
	prices := map[string]float64{
		"BTCUSDT": 65000,
		"ETHUSDT": 3500,
		"SOLUSDT": 160,
	}
	base := prices[symbol]
	if base == 0 {
		base = 100
	}
	// Add ±0.1% noise.
	noise := (rand.Float64() - 0.5) * 0.002 * base
	return decimal.NewFromFloat(base + noise).Round(2)
}
