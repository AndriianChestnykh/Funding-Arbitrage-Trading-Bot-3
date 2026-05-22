package venue

import (
	"context"

	"github.com/shopspring/decimal"

	"github.com/AndriianChestnykh/Funding-Arbitrage-Trading-Bot-3/internal/domain"
)

// Venue is the unified contract every exchange adapter implements.
// Each exchange wraps its native Go SDK behind this interface.
type Venue interface {
	// Name returns the exchange identifier, e.g. "binance".
	Name() string

	// SubscribeFunding streams normalized funding ticks for the given symbols.
	// The returned channel is closed when ctx is cancelled.
	SubscribeFunding(ctx context.Context, symbols []string) (<-chan domain.Tick, error)

	// PlaceOrder places an order. Idempotent via Order.ClientOrderID.
	PlaceOrder(ctx context.Context, o domain.Order) (domain.Fill, error)

	// ClosePosition closes an open position at market price.
	ClosePosition(ctx context.Context, symbol string, side domain.Side, size decimal.Decimal) (domain.Fill, error)
}
