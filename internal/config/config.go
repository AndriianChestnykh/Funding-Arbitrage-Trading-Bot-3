package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/shopspring/decimal"
)

type Config struct {
	ExecutionMode string // "paper" or "live"

	TelegramBotToken string
	TelegramChatID   int64

	DBPath string

	// Risk limits
	MaxPositionsPerSymbol int
	MaxGlobalNotionalUSDT decimal.Decimal
	ProposalTTL           time.Duration
	PreSettlementWindow   time.Duration // block new entries this close to funding
	MinNetEdgeBps         decimal.Decimal

	// Exchange credentials
	BinanceAPIKey    string
	BinanceAPISecret string
	BybitAPIKey      string
	BybitAPISecret   string
	OKXAPIKey        string
	OKXAPISecret     string
	OKXPassphrase    string
	HyperliquidKey   string

	// Cost model (per-venue taker fee + slippage, in basis points)
	TakerFeeBps decimal.Decimal
	SlippageBps decimal.Decimal

	// Symbols to monitor — base assets only, e.g. "BTC,ETH,SOL"
	Symbols []string

	// Per-trade notional size in USDT (paper or live)
	TradeNotionalUSDT decimal.Decimal
}

// Load reads configuration from environment variables (and .env file if present).
func Load() (*Config, error) {
	_ = godotenv.Load() // ignore error — .env is optional

	cfg := &Config{
		ExecutionMode:         env("EXECUTION_MODE", "paper"),
		TelegramBotToken:      env("TELEGRAM_BOT_TOKEN", ""),
		DBPath:                env("DB_PATH", "bot.db"),
		MaxPositionsPerSymbol: 1,
		ProposalTTL:           5 * time.Minute,
		PreSettlementWindow:   60 * time.Second,
		TakerFeeBps:           decimal.NewFromFloat(5),   // 0.05% per leg
		SlippageBps:           decimal.NewFromFloat(2),   // 0.02% per leg
		MinNetEdgeBps:         decimal.NewFromFloat(3),   // 0.03% minimum net edge
		TradeNotionalUSDT:     decimal.NewFromFloat(100), // $100 default
		MaxGlobalNotionalUSDT: decimal.NewFromFloat(1000),
		Symbols:               []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"},
	}

	if v := env("TELEGRAM_CHAT_ID", ""); v != "" {
		id, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid TELEGRAM_CHAT_ID: %w", err)
		}
		cfg.TelegramChatID = id
	}

	if v := env("MIN_NET_EDGE_BPS", ""); v != "" {
		bps, err := decimal.NewFromString(v)
		if err != nil {
			return nil, fmt.Errorf("invalid MIN_NET_EDGE_BPS: %w", err)
		}
		cfg.MinNetEdgeBps = bps
	}

	if v := env("TRADE_NOTIONAL_USDT", ""); v != "" {
		n, err := decimal.NewFromString(v)
		if err != nil {
			return nil, fmt.Errorf("invalid TRADE_NOTIONAL_USDT: %w", err)
		}
		cfg.TradeNotionalUSDT = n
	}

	if v := env("MAX_GLOBAL_NOTIONAL_USDT", ""); v != "" {
		n, err := decimal.NewFromString(v)
		if err != nil {
			return nil, fmt.Errorf("invalid MAX_GLOBAL_NOTIONAL_USDT: %w", err)
		}
		cfg.MaxGlobalNotionalUSDT = n
	}

	if v := env("SYMBOLS", ""); v != "" {
		cfg.Symbols = strings.Split(v, ",")
		for i := range cfg.Symbols {
			cfg.Symbols[i] = strings.TrimSpace(cfg.Symbols[i])
		}
	}

	cfg.BinanceAPIKey = env("BINANCE_API_KEY", "")
	cfg.BinanceAPISecret = env("BINANCE_API_SECRET", "")
	cfg.BybitAPIKey = env("BYBIT_API_KEY", "")
	cfg.BybitAPISecret = env("BYBIT_API_SECRET", "")
	cfg.OKXAPIKey = env("OKX_API_KEY", "")
	cfg.OKXAPISecret = env("OKX_API_SECRET", "")
	cfg.OKXPassphrase = env("OKX_PASSPHRASE", "")
	cfg.HyperliquidKey = env("HYPERLIQUID_KEY", "")

	if cfg.ExecutionMode == "live" {
		if cfg.TelegramBotToken == "" {
			return nil, fmt.Errorf("TELEGRAM_BOT_TOKEN is required in live mode")
		}
		if cfg.TelegramChatID == 0 {
			return nil, fmt.Errorf("TELEGRAM_CHAT_ID is required in live mode")
		}
	}

	return cfg, nil
}

// CostPerRoundTrip returns the total estimated cost (2 taker fills + 2× slippage) as a decimal fraction.
func (c *Config) CostPerRoundTrip() decimal.Decimal {
	bpsToFrac := decimal.NewFromInt(10000)
	fee := c.TakerFeeBps.Mul(decimal.NewFromInt(2)).Div(bpsToFrac)
	slip := c.SlippageBps.Mul(decimal.NewFromInt(2)).Div(bpsToFrac)
	return fee.Add(slip)
}

// MinNetEdge returns the minimum net edge as a decimal fraction.
func (c *Config) MinNetEdge() decimal.Decimal {
	return c.MinNetEdgeBps.Div(decimal.NewFromInt(10000))
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
