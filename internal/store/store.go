// Package store wraps the SQLite database.
// The strategy goroutine is the sole writer to the positions table.
// All other components read-only.
package store

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
	_ "modernc.org/sqlite"

	"github.com/AndriianChestnykh/Funding-Arbitrage-Trading-Bot-3/internal/domain"
)

//go:embed schema.sql
var schema string

// Store wraps the SQLite connection.
type Store struct {
	db *sql.DB
}

// Open opens (or creates) the SQLite database at path and applies the schema.
func Open(path string) (*Store, error) {
	dsn := fmt.Sprintf("%s?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000&_foreign_keys=on", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open %s: %w", path, err)
	}
	// WAL supports one writer + many readers; cap writers to 1 to avoid SQLITE_BUSY.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if _, err := db.Exec(schema); err != nil {
		return nil, fmt.Errorf("store: apply schema: %w", err)
	}
	return &Store{db: db}, nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

// --- Positions ---

// SavePosition inserts or replaces a position record.
func (s *Store) SavePosition(ctx context.Context, p domain.Position) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO positions
			(id, symbol, short_venue, long_venue, size,
			 entry_short_rate, entry_long_rate,
			 opened_at, funding_collected, status, last_checked_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			funding_collected = excluded.funding_collected,
			status            = excluded.status,
			last_checked_at   = excluded.last_checked_at,
			closed_at         = CASE WHEN excluded.status = 'closed' THEN datetime('now') ELSE closed_at END`,
		p.ID, p.Symbol, p.ShortVenue, p.LongVenue, p.Size.String(),
		p.EntryShortRate.String(), p.EntryLongRate.String(),
		p.OpenedAt.UTC(), p.FundingCollected.String(), string(p.Status),
		p.LastCheckedAt.UTC(),
	)
	return err
}

// OpenPositions returns all positions with status = "open".
func (s *Store) OpenPositions(ctx context.Context) ([]domain.Position, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, symbol, short_venue, long_venue, size,
		       entry_short_rate, entry_long_rate,
		       opened_at, funding_collected, status, last_checked_at
		FROM positions
		WHERE status = 'open'
		ORDER BY opened_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPositions(rows)
}

// AllPositions returns all positions (open and closed).
func (s *Store) AllPositions(ctx context.Context) ([]domain.Position, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, symbol, short_venue, long_venue, size,
		       entry_short_rate, entry_long_rate,
		       opened_at, funding_collected, status, last_checked_at
		FROM positions
		ORDER BY opened_at DESC
		LIMIT 50`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPositions(rows)
}

func scanPositions(rows *sql.Rows) ([]domain.Position, error) {
	var positions []domain.Position
	for rows.Next() {
		var p domain.Position
		var sizeStr, shortRateStr, longRateStr, fundingStr, statusStr string
		var openedAt, lastCheckedAt time.Time
		if err := rows.Scan(
			&p.ID, &p.Symbol, &p.ShortVenue, &p.LongVenue, &sizeStr,
			&shortRateStr, &longRateStr,
			&openedAt, &fundingStr, &statusStr, &lastCheckedAt,
		); err != nil {
			return nil, err
		}
		p.Size, _ = decimal.NewFromString(sizeStr)
		p.EntryShortRate, _ = decimal.NewFromString(shortRateStr)
		p.EntryLongRate, _ = decimal.NewFromString(longRateStr)
		p.FundingCollected, _ = decimal.NewFromString(fundingStr)
		p.Status = domain.PositionStatus(statusStr)
		p.OpenedAt = openedAt
		p.LastCheckedAt = lastCheckedAt
		positions = append(positions, p)
	}
	return positions, rows.Err()
}

// --- Fills ---

// SaveFill inserts a fill record linked to a position.
func (s *Store) SaveFill(ctx context.Context, positionID string, f domain.Fill) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO fills
			(id, position_id, order_id, venue, symbol, side, size, price, fee, filled_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		fmt.Sprintf("%s-%s", positionID, f.OrderID),
		positionID, f.OrderID, f.Venue, f.Symbol,
		string(f.Side), f.Size.String(), f.Price.String(), f.Fee.String(),
		f.FilledAt.UTC(),
	)
	return err
}

// --- Funding history ---

// SaveTick persists a funding tick to the history table (for backtest replay).
func (s *Store) SaveTick(ctx context.Context, t domain.Tick) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO funding_history (venue, symbol, rate, next_at, mark_price, received_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		t.Venue, t.Symbol, t.Rate.String(),
		t.NextAt.UTC(), t.MarkPrice.String(), t.Received.UTC(),
	)
	return err
}

// TickHistory returns funding history for a symbol/venue within a time window.
func (s *Store) TickHistory(ctx context.Context, venue, symbol string, from, to time.Time) ([]domain.Tick, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT venue, symbol, rate, next_at, mark_price, received_at
		FROM funding_history
		WHERE venue = ? AND symbol = ? AND received_at BETWEEN ? AND ?
		ORDER BY received_at`,
		venue, symbol, from.UTC(), to.UTC(),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ticks []domain.Tick
	for rows.Next() {
		var t domain.Tick
		var rateStr, markStr string
		if err := rows.Scan(&t.Venue, &t.Symbol, &rateStr, &t.NextAt, &markStr, &t.Received); err != nil {
			return nil, err
		}
		t.Rate, _ = decimal.NewFromString(rateStr)
		t.MarkPrice, _ = decimal.NewFromString(markStr)
		ticks = append(ticks, t)
	}
	return ticks, rows.Err()
}
