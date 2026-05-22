// Package binance implements the Venue interface for Binance USDT-M perpetual futures.
package binance

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	goBinance "github.com/adshao/go-binance/v2/futures"
	"github.com/shopspring/decimal"

	"github.com/AndriianChestnykh/Funding-Arbitrage-Trading-Bot-3/internal/domain"
)

const name = "binance"

// Venue is the Binance USDT-M perp adapter.
type Venue struct {
	client *goBinance.Client
}

// New creates a Binance futures venue. apiKey and apiSecret must be
// trade-only credentials with no withdrawal permissions.
func New(apiKey, apiSecret string) *Venue {
	return &Venue{client: goBinance.NewClient(apiKey, apiSecret)}
}

// Name implements venue.Venue.
func (v *Venue) Name() string { return name }

// SubscribeFunding subscribes to mark-price WS events for the given symbols.
// A single goroutine per symbol maintains an auto-reconnecting stream.
// The returned channel is closed when ctx is done.
func (v *Venue) SubscribeFunding(ctx context.Context, symbols []string) (<-chan domain.Tick, error) {
	out := make(chan domain.Tick, 64)

	go func() {
		defer close(out)

		// Fan-out: one reconnecting stream per symbol.
		inner := make(chan domain.Tick, 64)
		for _, sym := range symbols {
			sym := sym
			go streamSymbol(ctx, sym, inner)
		}

		for {
			select {
			case <-ctx.Done():
				return
			case tick, ok := <-inner:
				if !ok {
					return
				}
				select {
				case out <- tick:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return out, nil
}

// streamSymbol maintains a reconnecting WsMarkPriceServe for a single symbol,
// forwarding ticks to out until ctx is done.
func streamSymbol(ctx context.Context, symbol string, out chan<- domain.Tick) {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}

		errCh := make(chan error, 1)
		doneC, stopC, err := goBinance.WsMarkPriceServe(symbol,
			func(event *goBinance.WsMarkPriceEvent) {
				tick, ok := eventToTick(event)
				if !ok {
					return
				}
				select {
				case out <- tick:
				default:
				}
			},
			func(err error) {
				slog.Warn("binance: ws error", "symbol", symbol, "err", err)
				errCh <- err
			},
		)
		if err != nil {
			slog.Warn("binance: ws connect failed", "symbol", symbol, "err", err, "retry_in", backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, 60*time.Second)
			continue
		}
		backoff = time.Second // reset on successful connect

		// Wait until the stream closes, ctx is done, or an error arrives.
		select {
		case <-ctx.Done():
			close(stopC)
			<-doneC
			return
		case <-doneC:
			slog.Info("binance: ws stream closed, reconnecting", "symbol", symbol)
		case <-errCh:
			close(stopC)
			<-doneC
			slog.Info("binance: ws error, reconnecting", "symbol", symbol)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 60*time.Second)
	}
}

func eventToTick(e *goBinance.WsMarkPriceEvent) (domain.Tick, bool) {
	rate, err := decimal.NewFromString(e.FundingRate)
	if err != nil || e.FundingRate == "" {
		return domain.Tick{}, false
	}
	mark, err := decimal.NewFromString(e.MarkPrice)
	if err != nil {
		return domain.Tick{}, false
	}
	nextAt := time.UnixMilli(e.NextFundingTime)
	return domain.Tick{
		Venue:     name,
		Symbol:    e.Symbol,
		Rate:      rate,
		NextAt:    nextAt,
		MarkPrice: mark,
		Received:  time.Now(),
	}, true
}

// PlaceOrder places a single-leg order on Binance USDT-M futures.
// For limit orders, TIF is IOC (fill immediately or cancel).
func (v *Venue) PlaceOrder(ctx context.Context, o domain.Order) (domain.Fill, error) {
	svc := v.client.NewCreateOrderService().
		Symbol(o.Symbol).
		NewClientOrderID(o.ClientOrderID)

	switch o.Side {
	case domain.SideShort:
		svc = svc.Side(goBinance.SideTypeSell)
	default:
		svc = svc.Side(goBinance.SideTypeBuy)
	}

	switch o.Type {
	case domain.OrderTypeLimit:
		svc = svc.
			Type(goBinance.OrderTypeLimit).
			TimeInForce(goBinance.TimeInForceTypeIOC).
			Price(o.LimitPrice.StringFixed(8))
	default:
		svc = svc.Type(goBinance.OrderTypeMarket)
	}

	svc = svc.Quantity(o.Size.StringFixed(8))

	resp, err := svc.Do(ctx)
	if err != nil {
		return domain.Fill{}, fmt.Errorf("binance: place order %s: %w", o.ClientOrderID, err)
	}

	return fillFromResponse(o, resp)
}

// ClosePosition closes an open position at market price with ReduceOnly=true.
func (v *Venue) ClosePosition(ctx context.Context, symbol string, side domain.Side, size decimal.Decimal) (domain.Fill, error) {
	// To close: reverse the side.
	closeSide := goBinance.SideTypeBuy
	if side == domain.SideShort {
		closeSide = goBinance.SideTypeSell
	}

	resp, err := v.client.NewCreateOrderService().
		Symbol(symbol).
		Side(closeSide).
		Type(goBinance.OrderTypeMarket).
		Quantity(size.StringFixed(8)).
		ReduceOnly(true).
		Do(ctx)
	if err != nil {
		return domain.Fill{}, fmt.Errorf("binance: close position %s %s: %w", symbol, side, err)
	}

	o := domain.Order{Symbol: symbol, Side: side, Size: size}
	return fillFromResponse(o, resp)
}

func fillFromResponse(o domain.Order, resp *goBinance.CreateOrderResponse) (domain.Fill, error) {
	avgPrice, err := decimal.NewFromString(resp.AvgPrice)
	if err != nil || avgPrice.IsZero() {
		// For limit-IoC orders that partially fill or don't fill, use the order price.
		avgPrice = o.LimitPrice
	}

	execQty, err := decimal.NewFromString(resp.ExecutedQuantity)
	if err != nil {
		execQty = o.Size
	}

	// Binance futures taker fee is typically 0.04%; estimate it here since
	// the REST create-order endpoint does not return commission directly.
	// Actual commission can be reconciled via execution reports or account history.
	takerFeeBps := decimal.NewFromFloat(0.0004)
	fee := execQty.Mul(avgPrice).Mul(takerFeeBps)

	orderIDStr := strconv.FormatInt(resp.OrderID, 10)

	return domain.Fill{
		OrderID:  orderIDStr,
		Venue:    name,
		Symbol:   o.Symbol,
		Side:     o.Side,
		Size:     execQty,
		Price:    avgPrice,
		Fee:      fee,
		FilledAt: time.UnixMilli(resp.UpdateTime),
	}, nil
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
