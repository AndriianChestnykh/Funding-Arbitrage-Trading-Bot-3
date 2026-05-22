// Package bybit implements the Venue interface for Bybit V5 linear perpetual futures.
package bybit

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	bybitSDK "github.com/hirokisan/bybit/v2"
	"github.com/shopspring/decimal"

	"github.com/AndriianChestnykh/Funding-Arbitrage-Trading-Bot-3/internal/domain"
)

const name = "bybit"

// Venue is the Bybit V5 linear perp adapter.
type Venue struct {
	client *bybitSDK.Client
}

// New creates a Bybit venue. apiKey and apiSecret must be trade-only
// credentials with no withdrawal permissions.
func New(apiKey, apiSecret string) *Venue {
	return &Venue{
		client: bybitSDK.NewClient().WithAuth(apiKey, apiSecret),
	}
}

// Name implements venue.Venue.
func (v *Venue) Name() string { return name }

// SubscribeFunding subscribes to V5 linear ticker WS events for the given symbols.
// A reconnecting goroutine maintains the connection; the channel is closed when ctx is done.
func (v *Venue) SubscribeFunding(ctx context.Context, symbols []string) (<-chan domain.Tick, error) {
	out := make(chan domain.Tick, 64)
	go v.streamLoop(ctx, symbols, out)
	return out, nil
}

func (v *Venue) streamLoop(ctx context.Context, symbols []string, out chan<- domain.Tick) {
	defer close(out)

	keys := make([]bybitSDK.V5WebsocketPublicTickerParamKey, len(symbols))
	for i, sym := range symbols {
		keys[i] = bybitSDK.V5WebsocketPublicTickerParamKey{
			Symbol: bybitSDK.SymbolV5(sym),
		}
	}

	backoff := time.Second

	for {
		if ctx.Err() != nil {
			return
		}

		wsClient := bybitSDK.NewWebsocketClient()
		svc, err := wsClient.V5().Public(bybitSDK.CategoryV5Linear)
		if err != nil {
			slog.Warn("bybit: ws connect failed", "err", err, "retry_in", backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = minDur(backoff*2, 60*time.Second)
			continue
		}
		backoff = time.Second

		streamErr := make(chan error, 1)

		_, err = svc.SubscribeTickers(keys, func(resp bybitSDK.V5WebsocketPublicTickerResponse) error {
			li := resp.Data.LinearInverse
			if li == nil {
				return nil
			}
			tick, ok := linearTickerToTick(li)
			if !ok {
				return nil
			}
			select {
			case out <- tick:
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			return nil
		})
		if err != nil {
			slog.Warn("bybit: subscribe tickers failed", "err", err, "retry_in", backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = minDur(backoff*2, 60*time.Second)
			continue
		}

		// Run Start in a goroutine so we can also watch ctx.Done().
		go func() {
			streamErr <- svc.Start(ctx, func(isCloseError bool, err error) {
				if !isCloseError {
					slog.Warn("bybit: ws runtime error", "err", err)
				}
			})
		}()

		select {
		case <-ctx.Done():
			_ = svc.Close()
			return
		case err := <-streamErr:
			if ctx.Err() != nil {
				return
			}
			slog.Info("bybit: ws stream ended, reconnecting", "err", err, "retry_in", backoff)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = minDur(backoff*2, 60*time.Second)
	}
}

func linearTickerToTick(li *bybitSDK.V5WebsocketPublicTickerLinearInverseResult) (domain.Tick, bool) {
	if li.FundingRate == "" || li.NextFundingTime == "" {
		return domain.Tick{}, false
	}

	rate, err := decimal.NewFromString(li.FundingRate)
	if err != nil {
		return domain.Tick{}, false
	}

	mark, err := decimal.NewFromString(li.MarkPrice)
	if err != nil {
		return domain.Tick{}, false
	}

	nextMs, err := strconv.ParseInt(li.NextFundingTime, 10, 64)
	if err != nil {
		return domain.Tick{}, false
	}

	return domain.Tick{
		Venue:     name,
		Symbol:    string(li.Symbol),
		Rate:      rate,
		NextAt:    time.UnixMilli(nextMs),
		MarkPrice: mark,
		Received:  time.Now(),
	}, true
}

// PlaceOrder places a single-leg order on Bybit V5 linear perps.
// Limit orders use TIF=ImmediateOrCancel.
func (v *Venue) PlaceOrder(ctx context.Context, o domain.Order) (domain.Fill, error) {
	tif := bybitSDK.TimeInForceImmediateOrCancel
	oType := bybitSDK.OrderTypeMarket

	if o.Type == domain.OrderTypeLimit {
		oType = bybitSDK.OrderTypeLimit
	} else {
		tif = bybitSDK.TimeInForceGoodTillCancel
	}

	side := bybitSDK.SideBuy
	if o.Side == domain.SideShort {
		side = bybitSDK.SideSell
	}

	linkID := o.ClientOrderID
	qty := o.Size.StringFixed(8)

	param := bybitSDK.V5CreateOrderParam{
		Category:    bybitSDK.CategoryV5Linear,
		Symbol:      bybitSDK.SymbolV5(o.Symbol),
		Side:        side,
		OrderType:   oType,
		Qty:         qty,
		TimeInForce: &tif,
		OrderLinkID: &linkID,
	}

	if o.Type == domain.OrderTypeLimit {
		priceStr := o.LimitPrice.StringFixed(8)
		param.Price = &priceStr
	}

	resp, err := v.client.V5().Order().CreateOrder(param)
	if err != nil {
		return domain.Fill{}, fmt.Errorf("bybit: place order %s: %w", o.ClientOrderID, err)
	}

	// Bybit's create-order response only returns the order ID; fill price is
	// not available immediately. We approximate using the last mark price tick.
	// For a production system, query execution history for exact fill data.
	fill := domain.Fill{
		OrderID:  resp.Result.OrderID,
		Venue:    name,
		Symbol:   o.Symbol,
		Side:     o.Side,
		Size:     o.Size,
		Price:    o.LimitPrice, // best approximation at order time
		FilledAt: time.Now(),
	}

	// Estimate taker fee: 0.055% for Bybit linear
	takerFeeBps := decimal.NewFromFloat(0.00055)
	fill.Fee = fill.Size.Mul(fill.Price).Mul(takerFeeBps)

	return fill, nil
}

// ClosePosition closes an open position at market with ReduceOnly=true.
func (v *Venue) ClosePosition(ctx context.Context, symbol string, side domain.Side, size decimal.Decimal) (domain.Fill, error) {
	// To close: reverse the side.
	closeSide := bybitSDK.SideBuy
	if side == domain.SideShort {
		closeSide = bybitSDK.SideSell
	}

	tif := bybitSDK.TimeInForceGoodTillCancel
	reduceOnly := true

	resp, err := v.client.V5().Order().CreateOrder(bybitSDK.V5CreateOrderParam{
		Category:    bybitSDK.CategoryV5Linear,
		Symbol:      bybitSDK.SymbolV5(symbol),
		Side:        closeSide,
		OrderType:   bybitSDK.OrderTypeMarket,
		Qty:         size.StringFixed(8),
		TimeInForce: &tif,
		ReduceOnly:  &reduceOnly,
	})
	if err != nil {
		return domain.Fill{}, fmt.Errorf("bybit: close position %s %s: %w", symbol, side, err)
	}

	fill := domain.Fill{
		OrderID:  resp.Result.OrderID,
		Venue:    name,
		Symbol:   symbol,
		Side:     side,
		Size:     size,
		FilledAt: time.Now(),
	}

	return fill, nil
}

func minDur(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
