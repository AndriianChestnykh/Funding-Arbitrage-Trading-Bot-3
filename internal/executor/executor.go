// Package executor places the two legs of an arbitrage trade as atomically as possible.
package executor

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/shopspring/decimal"

	"github.com/AndriianChestnykh/Funding-Arbitrage-Trading-Bot-3/internal/domain"
	"github.com/AndriianChestnykh/Funding-Arbitrage-Trading-Bot-3/internal/venue"
)

// Executor routes orders to the correct venue adapters.
type Executor struct {
	venues     map[string]venue.Venue
	legTimeout time.Duration
}

// New creates an Executor with the provided venue adapters.
func New(venues []venue.Venue) *Executor {
	m := make(map[string]venue.Venue, len(venues))
	for _, v := range venues {
		m[v.Name()] = v
	}
	return &Executor{venues: m, legTimeout: 10 * time.Second}
}

// OpenResult holds the fills from a successful open execution.
type OpenResult struct {
	ShortFill domain.Fill
	LongFill  domain.Fill
}

// Open opens a delta-neutral position: short on candidate.ShortVenue, long on candidate.LongVenue.
// Execution order: less-liquid (short) leg first. On leg-B failure, leg-A is closed at market.
func (e *Executor) Open(ctx context.Context, c domain.Candidate, size decimal.Decimal) (OpenResult, error) {
	shortVenue, ok := e.venues[c.ShortVenue]
	if !ok {
		return OpenResult{}, fmt.Errorf("executor: unknown venue %q", c.ShortVenue)
	}
	longVenue, ok := e.venues[c.LongVenue]
	if !ok {
		return OpenResult{}, fmt.Errorf("executor: unknown venue %q", c.LongVenue)
	}

	clientID := fmt.Sprintf("fab-%d", time.Now().UnixMilli())

	// Leg A: short on shortVenue (limit-IoC for price control).
	legACtx, cancel := context.WithTimeout(ctx, e.legTimeout)
	defer cancel()

	shortOrder := domain.Order{
		ClientOrderID: clientID + "-short",
		Venue:         c.ShortVenue,
		Symbol:        c.Symbol,
		Side:          domain.SideShort,
		Size:          size,
		Type:          domain.OrderTypeLimit,
		LimitPrice:    decimal.Zero, // executor should pass bid price; simplified here
	}
	shortFill, err := shortVenue.PlaceOrder(legACtx, shortOrder)
	if err != nil {
		return OpenResult{}, fmt.Errorf("executor: leg A (short %s on %s): %w", c.Symbol, c.ShortVenue, err)
	}
	slog.Info("executor: leg A filled", "venue", c.ShortVenue, "side", "short", "price", shortFill.Price)

	// Leg B: long on longVenue (market for immediacy after leg A is confirmed).
	legBCtx, cancel2 := context.WithTimeout(ctx, e.legTimeout)
	defer cancel2()

	longOrder := domain.Order{
		ClientOrderID: clientID + "-long",
		Venue:         c.LongVenue,
		Symbol:        c.Symbol,
		Side:          domain.SideLong,
		Size:          size,
		Type:          domain.OrderTypeMarket,
	}
	longFill, err := longVenue.PlaceOrder(legBCtx, longOrder)
	if err != nil {
		// Leg B failed — unwind leg A at market to avoid naked exposure.
		slog.Error("executor: leg B failed, unwinding leg A", "err", err)
		unwindCtx, cancelUnwind := context.WithTimeout(ctx, e.legTimeout)
		defer cancelUnwind()
		if _, unwindErr := shortVenue.ClosePosition(unwindCtx, c.Symbol, domain.SideShort, size); unwindErr != nil {
			slog.Error("executor: UNWIND FAILED — manual close required",
				"venue", c.ShortVenue, "symbol", c.Symbol, "err", unwindErr)
		}
		return OpenResult{}, fmt.Errorf("executor: leg B (long %s on %s): %w", c.Symbol, c.LongVenue, err)
	}
	slog.Info("executor: leg B filled", "venue", c.LongVenue, "side", "long", "price", longFill.Price)

	return OpenResult{ShortFill: shortFill, LongFill: longFill}, nil
}

// Close closes both legs of an open position at market.
func (e *Executor) Close(ctx context.Context, p domain.Position) (shortFill, longFill domain.Fill, err error) {
	shortVenue, ok := e.venues[p.ShortVenue]
	if !ok {
		return domain.Fill{}, domain.Fill{}, fmt.Errorf("executor: unknown venue %q", p.ShortVenue)
	}
	longVenue, ok := e.venues[p.LongVenue]
	if !ok {
		return domain.Fill{}, domain.Fill{}, fmt.Errorf("executor: unknown venue %q", p.LongVenue)
	}

	closeCtx, cancel := context.WithTimeout(ctx, e.legTimeout*2)
	defer cancel()

	shortFill, err = shortVenue.ClosePosition(closeCtx, p.Symbol, domain.SideShort, p.Size)
	if err != nil {
		return domain.Fill{}, domain.Fill{}, fmt.Errorf("executor: close short leg on %s: %w", p.ShortVenue, err)
	}

	longFill, err = longVenue.ClosePosition(closeCtx, p.Symbol, domain.SideLong, p.Size)
	if err != nil {
		slog.Error("executor: LONG LEG CLOSE FAILED — manual close required",
			"venue", p.LongVenue, "symbol", p.Symbol, "size", p.Size, "err", err)
		return shortFill, domain.Fill{}, fmt.Errorf("executor: close long leg on %s: %w", p.LongVenue, err)
	}

	slog.Info("executor: position closed", "id", p.ID, "symbol", p.Symbol)
	return shortFill, longFill, nil
}
