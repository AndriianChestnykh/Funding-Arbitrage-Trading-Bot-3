-- Funding Arbitrage Bot — SQLite schema
-- WAL mode is set at connection open time, not here.

CREATE TABLE IF NOT EXISTS positions (
    id                 TEXT PRIMARY KEY,
    symbol             TEXT NOT NULL,
    short_venue        TEXT NOT NULL,
    long_venue         TEXT NOT NULL,
    size               TEXT NOT NULL,  -- decimal string
    entry_short_rate   TEXT NOT NULL,
    entry_long_rate    TEXT NOT NULL,
    opened_at          DATETIME NOT NULL,
    funding_collected  TEXT NOT NULL DEFAULT '0',
    status             TEXT NOT NULL DEFAULT 'open',  -- open | closed
    last_checked_at    DATETIME NOT NULL,
    closed_at          DATETIME
);

CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);
CREATE INDEX IF NOT EXISTS idx_positions_symbol  ON positions(symbol, status);

CREATE TABLE IF NOT EXISTS fills (
    id          TEXT PRIMARY KEY,
    position_id TEXT REFERENCES positions(id),
    order_id    TEXT NOT NULL,
    venue       TEXT NOT NULL,
    symbol      TEXT NOT NULL,
    side        TEXT NOT NULL,  -- long | short
    size        TEXT NOT NULL,
    price       TEXT NOT NULL,
    fee         TEXT NOT NULL,
    filled_at   DATETIME NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fills_position ON fills(position_id);

CREATE TABLE IF NOT EXISTS funding_history (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    venue      TEXT NOT NULL,
    symbol     TEXT NOT NULL,
    rate       TEXT NOT NULL,
    next_at    DATETIME NOT NULL,
    mark_price TEXT NOT NULL,
    received_at DATETIME NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_funding_history_lookup
    ON funding_history(venue, symbol, received_at DESC);
