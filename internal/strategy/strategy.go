// Package strategy is the single-owner goroutine that holds all position state in RAM,
// mirrors every change to SQLite, and orchestrates the approve→execute→monitor loop.
package strategy

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/shopspring/decimal"

	"github.com/AndriianChestnykh/Funding-Arbitrage-Trading-Bot-3/internal/config"
	"github.com/AndriianChestnykh/Funding-Arbitrage-Trading-Bot-3/internal/domain"
	"github.com/AndriianChestnykh/Funding-Arbitrage-Trading-Bot-3/internal/executor"
	"github.com/AndriianChestnykh/Funding-Arbitrage-Trading-Bot-3/internal/store"
)

// TelegramNotifier is the subset of the Telegram bot the strategy needs.
type TelegramNotifier interface {
	SendProposal(ctx context.Context, p domain.Proposal) error
	SendPositionAlert(ctx context.Context, p domain.Position, reason string) error
	SendText(ctx context.Context, text string) error
}

// Strategy manages open positions and orchestrates the full trade lifecycle.
type Strategy struct {
	cfg         *config.Config
	store       *store.Store
	exec        *executor.Executor
	tg          TelegramNotifier
	positions   map[string]domain.Position // id → position (in-RAM)
	proposals   map[string]domain.Proposal // id → pending proposal
	snoozed     map[string]time.Time        // proposalID → snooze-until
	latestTicks map[string]map[string]domain.Tick // venue → symbol → latest tick
	mu          sync.Mutex
}

// New creates a Strategy and rehydrates open positions from SQLite.
func New(
	cfg *config.Config,
	st *store.Store,
	exec *executor.Executor,
	tg TelegramNotifier,
) (*Strategy, error) {
	s := &Strategy{
		cfg:         cfg,
		store:       st,
		exec:        exec,
		tg:          tg,
		positions:   make(map[string]domain.Position),
		proposals:   make(map[string]domain.Proposal),
		snoozed:     make(map[string]time.Time),
		latestTicks: make(map[string]map[string]domain.Tick),
	}

	// Rehydrate open positions on startup.
	open, err := st.OpenPositions(context.Background())
	if err != nil {
		return nil, fmt.Errorf("strategy: rehydrate positions: %w", err)
	}
	for _, p := range open {
		s.positions[p.ID] = p
		slog.Info("strategy: rehydrated open position", "id", p.ID, "symbol", p.Symbol)
	}

	return s, nil
}

// Run is the main event loop. It processes candidates, approvals, close requests,
// and monitors open positions on every tick. Single goroutine — no locks needed on hot path.
func (s *Strategy) Run(
	ctx context.Context,
	candidateCh <-chan domain.Candidate,
	tickCh <-chan domain.Tick,
	approvalCh <-chan domain.Approval,
	closeCh <-chan string,
) {
	slog.Info("strategy: started", "mode", s.cfg.ExecutionMode, "open_positions", len(s.positions))
	cleanupTicker := time.NewTicker(time.Minute)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("strategy: shutting down")
			return

		case tick, ok := <-tickCh:
			if !ok {
				return
			}
			s.updateTick(tick)
			s.monitorPositions(ctx, tick)

		case candidate, ok := <-candidateCh:
			if !ok {
				return
			}
			s.handleCandidate(ctx, candidate)

		case approval, ok := <-approvalCh:
			if !ok {
				return
			}
			s.handleApproval(ctx, approval)

		case posID, ok := <-closeCh:
			if !ok {
				return
			}
			s.handleCloseRequest(ctx, posID)

		case <-cleanupTicker.C:
			s.expireProposals(ctx)
		}
	}
}

func (s *Strategy) updateTick(tick domain.Tick) {
	if s.latestTicks[tick.Venue] == nil {
		s.latestTicks[tick.Venue] = make(map[string]domain.Tick)
	}
	s.latestTicks[tick.Venue][tick.Symbol] = tick
}

func (s *Strategy) handleCandidate(ctx context.Context, c domain.Candidate) {
	// Check: already have a position for this symbol pair.
	for _, p := range s.positions {
		if p.Symbol == c.Symbol && p.ShortVenue == c.ShortVenue && p.LongVenue == c.LongVenue {
			slog.Debug("strategy: skipping — position already open",
				"symbol", c.Symbol, "short", c.ShortVenue, "long", c.LongVenue)
			return
		}
	}

	// Check: global notional cap.
	totalNotional := s.totalOpenNotional()
	if totalNotional.GreaterThanOrEqual(s.cfg.MaxGlobalNotionalUSDT) {
		slog.Debug("strategy: skipping — global notional cap reached", "total", totalNotional)
		return
	}

	// Check: per-symbol cap.
	symbolCount := 0
	for _, p := range s.positions {
		if p.Symbol == c.Symbol {
			symbolCount++
		}
	}
	if symbolCount >= s.cfg.MaxPositionsPerSymbol {
		slog.Debug("strategy: skipping — per-symbol cap reached", "symbol", c.Symbol)
		return
	}

	proposal := domain.Proposal{
		ID:        fmt.Sprintf("prop-%d", time.Now().UnixNano()),
		Candidate: c,
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(s.cfg.ProposalTTL),
	}

	// Check: not snoozed.
	if until, ok := s.snoozed[pairKey(c)]; ok && time.Now().Before(until) {
		slog.Debug("strategy: skipping — snoozed", "symbol", c.Symbol, "until", until)
		return
	}

	s.proposals[proposal.ID] = proposal

	if err := s.tg.SendProposal(ctx, proposal); err != nil {
		slog.Error("strategy: failed to send proposal", "err", err)
		delete(s.proposals, proposal.ID)
		return
	}
	slog.Info("strategy: proposal sent", "id", proposal.ID,
		"symbol", c.Symbol, "net_edge_bps", c.NetEdge.Mul(decimal.NewFromInt(10000)).StringFixed(2))
}

func (s *Strategy) handleApproval(ctx context.Context, a domain.Approval) {
	proposal, ok := s.proposals[a.ProposalID]
	if !ok {
		slog.Warn("strategy: received approval for unknown proposal", "id", a.ProposalID)
		return
	}

	if a.Snoozed {
		s.snoozed[pairKey(proposal.Candidate)] = time.Now().Add(a.SnoozedFor)
		delete(s.proposals, a.ProposalID)
		slog.Info("strategy: proposal snoozed", "id", a.ProposalID, "for", a.SnoozedFor)
		return
	}

	if !a.Approved {
		delete(s.proposals, a.ProposalID)
		slog.Info("strategy: proposal rejected", "id", a.ProposalID)
		return
	}

	// Expired?
	if time.Now().After(proposal.ExpiresAt) {
		delete(s.proposals, a.ProposalID)
		_ = s.tg.SendText(ctx, fmt.Sprintf("⏰ Proposal %s expired before execution.", a.ProposalID))
		return
	}

	delete(s.proposals, a.ProposalID)

	// Execute.
	go s.execute(ctx, proposal)
}

func (s *Strategy) execute(ctx context.Context, proposal domain.Proposal) {
	c := proposal.Candidate
	result, err := s.exec.Open(ctx, c, s.cfg.TradeNotionalUSDT)
	if err != nil {
		slog.Error("strategy: execution failed", "proposal", proposal.ID, "err", err)
		_ = s.tg.SendText(ctx, fmt.Sprintf("❌ Execution failed for %s: %s", c.Symbol, err))
		return
	}

	pos := domain.Position{
		ID:             proposal.ID,
		Symbol:         c.Symbol,
		ShortVenue:     c.ShortVenue,
		LongVenue:      c.LongVenue,
		Size:           s.cfg.TradeNotionalUSDT,
		EntryShortRate: c.ShortRate,
		EntryLongRate:  c.LongRate,
		OpenedAt:       time.Now(),
		Status:         domain.StatusOpen,
		LastCheckedAt:  time.Now(),
	}

	// Mirror to SQLite.
	if err := s.store.SavePosition(ctx, pos); err != nil {
		slog.Error("strategy: failed to save position", "err", err)
	}
	if err := s.store.SaveFill(ctx, pos.ID, result.ShortFill); err != nil {
		slog.Error("strategy: failed to save short fill", "err", err)
	}
	if err := s.store.SaveFill(ctx, pos.ID, result.LongFill); err != nil {
		slog.Error("strategy: failed to save long fill", "err", err)
	}

	// Add to in-RAM state (from the strategy goroutine — no lock needed here
	// because execute is called via go, but writes must still be thread-safe).
	s.mu.Lock()
	s.positions[pos.ID] = pos
	s.mu.Unlock()

	slog.Info("strategy: position opened",
		"id", pos.ID, "symbol", pos.Symbol,
		"short_venue", pos.ShortVenue, "long_venue", pos.LongVenue)
	_ = s.tg.SendText(ctx, fmt.Sprintf("✅ Position opened: %s on %s/%s", pos.Symbol, pos.ShortVenue, pos.LongVenue))
}

func (s *Strategy) handleCloseRequest(ctx context.Context, posID string) {
	pos, ok := s.positions[posID]
	if !ok {
		slog.Warn("strategy: close request for unknown position", "id", posID)
		return
	}
	go s.closePosition(ctx, pos, "user requested")
}

func (s *Strategy) closePosition(ctx context.Context, pos domain.Position, reason string) {
	shortFill, longFill, err := s.exec.Close(ctx, pos)
	if err != nil {
		slog.Error("strategy: close failed", "id", pos.ID, "err", err)
		_ = s.tg.SendText(ctx, fmt.Sprintf("❌ Close failed for %s: %s — manual action required", pos.ID, err))
		return
	}

	pos.Status = domain.StatusClosed
	pos.LastCheckedAt = time.Now()

	s.mu.Lock()
	delete(s.positions, pos.ID)
	s.mu.Unlock()

	if err := s.store.SavePosition(ctx, pos); err != nil {
		slog.Error("strategy: failed to update closed position", "err", err)
	}
	_ = s.store.SaveFill(ctx, pos.ID, shortFill)
	_ = s.store.SaveFill(ctx, pos.ID, longFill)

	slog.Info("strategy: position closed", "id", pos.ID, "reason", reason)
	_ = s.tg.SendText(ctx, fmt.Sprintf("🔴 Position %s closed (%s). Funding collected: %s USDT",
		pos.ID, reason, pos.FundingCollected.StringFixed(4)))
}

// monitorPositions checks all open positions against the latest tick for exit conditions.
func (s *Strategy) monitorPositions(ctx context.Context, tick domain.Tick) {
	s.mu.Lock()
	positions := make([]domain.Position, 0, len(s.positions))
	for _, p := range s.positions {
		positions = append(positions, p)
	}
	s.mu.Unlock()

	for _, pos := range positions {
		if pos.Symbol != tick.Symbol {
			continue
		}
		shortTick, hasShort := s.latestTicks[pos.ShortVenue][pos.Symbol]
		longTick, hasLong := s.latestTicks[pos.LongVenue][pos.Symbol]
		if !hasShort || !hasLong {
			continue
		}

		spread := shortTick.Rate.Sub(longTick.Rate)
		netEdge := spread.Sub(s.cfg.CostPerRoundTrip())
		reason := ""

		switch {
		case spread.LessThan(decimal.Zero):
			reason = fmt.Sprintf("funding spread flipped (spread=%s bps)",
				spread.Mul(decimal.NewFromInt(10000)).StringFixed(2))
		case netEdge.LessThan(s.cfg.MinNetEdge()):
			reason = fmt.Sprintf("net edge below threshold (%s bps)",
				netEdge.Mul(decimal.NewFromInt(10000)).StringFixed(2))
		}

		if reason != "" {
			slog.Info("strategy: exit condition triggered", "id", pos.ID, "reason", reason)
			if err := s.tg.SendPositionAlert(ctx, pos, reason); err != nil {
				slog.Error("strategy: failed to send position alert", "err", err)
			}
		}

		// Update last checked timestamp.
		pos.LastCheckedAt = time.Now()
		s.mu.Lock()
		s.positions[pos.ID] = pos
		s.mu.Unlock()
	}
}

func (s *Strategy) expireProposals(ctx context.Context) {
	now := time.Now()
	for id, p := range s.proposals {
		if now.After(p.ExpiresAt) {
			delete(s.proposals, id)
			slog.Info("strategy: proposal expired", "id", id)
		}
	}
}

func (s *Strategy) totalOpenNotional() decimal.Decimal {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := decimal.Zero
	for _, p := range s.positions {
		total = total.Add(p.Size)
	}
	return total
}

func pairKey(c domain.Candidate) string {
	return c.Symbol + ":" + c.ShortVenue + ":" + c.LongVenue
}
