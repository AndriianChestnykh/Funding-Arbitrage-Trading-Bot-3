package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"

	"github.com/AndriianChestnykh/Funding-Arbitrage-Trading-Bot-3/internal/config"
	"github.com/AndriianChestnykh/Funding-Arbitrage-Trading-Bot-3/internal/domain"
	"github.com/AndriianChestnykh/Funding-Arbitrage-Trading-Bot-3/internal/engine"
	"github.com/AndriianChestnykh/Funding-Arbitrage-Trading-Bot-3/internal/executor"
	"github.com/AndriianChestnykh/Funding-Arbitrage-Trading-Bot-3/internal/store"
	"github.com/AndriianChestnykh/Funding-Arbitrage-Trading-Bot-3/internal/strategy"
	"github.com/AndriianChestnykh/Funding-Arbitrage-Trading-Bot-3/internal/telegram"
	"github.com/AndriianChestnykh/Funding-Arbitrage-Trading-Bot-3/internal/venue"
	"github.com/AndriianChestnykh/Funding-Arbitrage-Trading-Bot-3/internal/venue/binance"
	"github.com/AndriianChestnykh/Funding-Arbitrage-Trading-Bot-3/internal/venue/bybit"
	"github.com/AndriianChestnykh/Funding-Arbitrage-Trading-Bot-3/internal/venue/sim"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	slog.Info("starting fab-bot", "mode", cfg.ExecutionMode, "db", cfg.DBPath)

	// Open SQLite store.
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	defer st.Close()

	// Build venue adapters.
	venues := buildVenues(cfg)
	slog.Info("venues loaded", "count", len(venues))

	// Channels — all bounded to provide backpressure.
	tickCh := make(chan domain.Tick, 1024)
	candidateCh := make(chan domain.Candidate, 64)
	approvalCh := make(chan domain.Approval, 16)
	closeCh := make(chan string, 16)

	// Telegram bot.
	tgBot, err := telegram.New(cfg.TelegramBotToken, cfg.TelegramChatID, approvalCh, closeCh)
	if err != nil {
		return fmt.Errorf("telegram: %w", err)
	}

	// Opportunity engine.
	eng := engine.New(engine.Config{
		MinNetEdge:          cfg.MinNetEdge(),
		CostPerRoundTrip:    cfg.CostPerRoundTrip(),
		PreSettlementWindow: cfg.PreSettlementWindow,
	})

	// Executor.
	exec := executor.New(venues)

	// Strategy (rehydrates open positions from SQLite on startup).
	strat, err := strategy.New(cfg, st, exec, tgBot)
	if err != nil {
		return fmt.Errorf("strategy: %w", err)
	}

	// Root context — cancelled on SIGINT/SIGTERM.
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	g, gctx := errgroup.WithContext(ctx)

	// Fan-in: start one goroutine per venue to subscribe and forward ticks to tickCh.
	for _, v := range venues {
		v := v
		g.Go(func() error {
			ch, err := v.SubscribeFunding(gctx, cfg.Symbols)
			if err != nil {
				return fmt.Errorf("venue %s subscribe: %w", v.Name(), err)
			}
			slog.Info("ingestor: subscribed", "venue", v.Name(), "symbols", cfg.Symbols)
			for {
				select {
				case <-gctx.Done():
					return nil
				case tick, ok := <-ch:
					if !ok {
						return nil
					}
					// Persist to history (best-effort, non-blocking).
					go func(t domain.Tick) {
						if err := st.SaveTick(context.Background(), t); err != nil {
							slog.Warn("store: failed to save tick", "err", err)
						}
					}(tick)
					select {
					case tickCh <- tick:
					default:
						slog.Warn("tick channel full — dropping", "venue", v.Name(), "symbol", tick.Symbol)
					}
				}
			}
		})
	}

	// Fan-out: duplicate tickCh to both engine and strategy.
	// We use a broadcast pattern: engine reads candidateCh, strategy reads tickCh directly.
	// To avoid duplicating the channel, we share the same tickCh by using a splitter goroutine.
	tickForEngine := make(chan domain.Tick, 512)
	tickForStrategy := make(chan domain.Tick, 512)

	g.Go(func() error {
		defer close(tickForEngine)
		defer close(tickForStrategy)
		for {
			select {
			case <-gctx.Done():
				return nil
			case tick, ok := <-tickCh:
				if !ok {
					return nil
				}
				select {
				case tickForEngine <- tick:
				default:
				}
				select {
				case tickForStrategy <- tick:
				default:
				}
			}
		}
	})

	// Engine goroutine.
	g.Go(func() error {
		eng.Run(gctx, tickForEngine, candidateCh)
		return nil
	})

	// Strategy goroutine (single owner of positions).
	g.Go(func() error {
		strat.Run(gctx, candidateCh, tickForStrategy, approvalCh, closeCh)
		return nil
	})

	// Telegram bot goroutine.
	g.Go(func() error {
		return tgBot.Run(gctx)
	})

	slog.Info("fab-bot running — press Ctrl+C to stop")
	return g.Wait()
}

// buildVenues constructs venue adapters based on config.
// In paper mode all venues are simulators. In live mode,
// replace sim.New() calls with the real exchange adapters.
func buildVenues(cfg *config.Config) []venue.Venue {
	if cfg.ExecutionMode != "live" {
		return []venue.Venue{
			// Two sim venues with different rate biases to generate spread.
			sim.New("binance-sim", 5),    // +0.05% bias (bullish)
			sim.New("bybit-sim", -2),     // -0.02% bias (bearish)
			sim.New("okx-sim", 1),
			sim.New("hyperliquid-sim", 3),
		}
	}

	return []venue.Venue{
		binance.New(cfg.BinanceAPIKey, cfg.BinanceAPISecret),
		bybit.New(cfg.BybitAPIKey, cfg.BybitAPISecret),
	}
}
