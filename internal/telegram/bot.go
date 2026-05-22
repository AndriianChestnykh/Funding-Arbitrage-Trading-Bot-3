// Package telegram provides the Telegram approval UX for the bot.
package telegram

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/mymmrac/telego"
	tu "github.com/mymmrac/telego/telegoutil"
	"github.com/shopspring/decimal"

	"github.com/AndriianChestnykh/Funding-Arbitrage-Trading-Bot-3/internal/domain"
)

const (
	callbackApprove = "approve"
	callbackReject  = "reject"
	callbackSnooze  = "snooze"
	callbackClose   = "close"
)

// Bot wraps the Telegram bot and handles proposal / approval flow.
type Bot struct {
	bot        *telego.Bot
	chatID     int64
	approvalCh chan<- domain.Approval
	closeCh    chan<- string // position IDs to close
}

// New creates a Bot. approvalCh receives user decisions on proposals.
// closeCh receives position IDs the user wants to close.
func New(token string, chatID int64, approvalCh chan<- domain.Approval, closeCh chan<- string) (*Bot, error) {
	if token == "" {
		return &Bot{chatID: chatID, approvalCh: approvalCh, closeCh: closeCh}, nil
	}
	b, err := telego.NewBot(token)
	if err != nil {
		return nil, fmt.Errorf("telegram: %w", err)
	}
	return &Bot{bot: b, chatID: chatID, approvalCh: approvalCh, closeCh: closeCh}, nil
}

// Run starts the Telegram update loop until ctx is done.
func (b *Bot) Run(ctx context.Context) error {
	if b.bot == nil {
		slog.Info("telegram: no bot token — approval UX disabled (paper mode)")
		<-ctx.Done()
		return nil
	}

	updates, err := b.bot.UpdatesViaLongPolling(ctx, nil)
	if err != nil {
		return fmt.Errorf("telegram: long polling: %w", err)
	}

	slog.Info("telegram: bot started")
	for {
		select {
		case <-ctx.Done():
			return nil
		case update, ok := <-updates:
			if !ok {
				return nil
			}
			b.handleUpdate(ctx, update)
		}
	}
}

func (b *Bot) handleUpdate(ctx context.Context, update telego.Update) {
	if update.CallbackQuery == nil {
		if update.Message != nil {
			b.handleCommand(ctx, update.Message)
		}
		return
	}
	cb := update.CallbackQuery
	data := cb.Data
	if len(data) == 0 {
		return
	}

	var proposalID string
	var action string
	// data format: "action:proposalID"
	if n, _ := fmt.Sscanf(data, "%s", &data); n == 0 {
		return
	}
	// Simple split on ":"
	for i, c := range data {
		if c == ':' {
			action = data[:i]
			proposalID = data[i+1:]
			break
		}
	}

	switch action {
	case callbackApprove:
		slog.Info("telegram: user approved proposal", "id", proposalID)
		select {
		case b.approvalCh <- domain.Approval{ProposalID: proposalID, Approved: true}:
		case <-ctx.Done():
		}
	case callbackReject:
		slog.Info("telegram: user rejected proposal", "id", proposalID)
		select {
		case b.approvalCh <- domain.Approval{ProposalID: proposalID, Approved: false}:
		case <-ctx.Done():
		}
	case callbackSnooze:
		slog.Info("telegram: user snoozed proposal", "id", proposalID)
		select {
		case b.approvalCh <- domain.Approval{ProposalID: proposalID, Snoozed: true, SnoozedFor: 10 * time.Minute}:
		case <-ctx.Done():
		}
	case callbackClose:
		slog.Info("telegram: user requested close", "position_id", proposalID)
		select {
		case b.closeCh <- proposalID:
		case <-ctx.Done():
		}
	}

	// Acknowledge the callback to remove the loading spinner.
	if b.bot != nil {
		_ = b.bot.AnswerCallbackQuery(ctx, &telego.AnswerCallbackQueryParams{
			CallbackQueryID: cb.ID,
		})
	}
}

func (b *Bot) handleCommand(_ context.Context, msg *telego.Message) {
	if msg.Text == "/positions" || msg.Text == "/status" {
		// The strategy goroutine will respond to these separately.
		// Here we just log.
		slog.Info("telegram: received command", "text", msg.Text)
	}
}

// SendProposal sends a funding opportunity proposal to the configured chat.
func (b *Bot) SendProposal(ctx context.Context, p domain.Proposal) error {
	c := p.Candidate
	annualPct := c.AnnualizedEdgePct()
	text := fmt.Sprintf(
		"🎯 *Funding Opportunity*\n\n"+
			"Symbol: `%s`\n"+
			"Short: `%s` @ `%s%%` / 8h\n"+
			"Long:  `%s` @ `%s%%` / 8h\n"+
			"Net edge: `%s bps` ≈ `%s%%/yr`\n"+
			"TTF: `%s`\n\n"+
			"Expires in: `%s`",
		c.Symbol,
		c.ShortVenue, c.ShortRate.Mul(decimal.NewFromInt(100)).StringFixed(4),
		c.LongVenue, c.LongRate.Mul(decimal.NewFromInt(100)).StringFixed(4),
		c.NetEdge.Mul(decimal.NewFromInt(10000)).StringFixed(2),
		annualPct.StringFixed(1),
		c.TTF.Round(time.Minute),
		time.Until(p.ExpiresAt).Round(time.Second),
	)

	if b.bot == nil {
		slog.Info("telegram: [paper] would send proposal", "text", text)
		return nil
	}

	keyboard := tu.InlineKeyboard(
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton("✅ Approve").WithCallbackData(callbackApprove+":"+p.ID),
			tu.InlineKeyboardButton("❌ Reject").WithCallbackData(callbackReject+":"+p.ID),
			tu.InlineKeyboardButton("⏸ Snooze 10m").WithCallbackData(callbackSnooze+":"+p.ID),
		),
	)

	_, err := b.bot.SendMessage(ctx, &telego.SendMessageParams{
		ChatID:      tu.ID(b.chatID),
		Text:        text,
		ParseMode:   "Markdown",
		ReplyMarkup: keyboard,
	})
	return err
}

// SendPositionAlert sends a degradation warning for an open position.
func (b *Bot) SendPositionAlert(ctx context.Context, p domain.Position, reason string) error {
	text := fmt.Sprintf(
		"⚠️ *Position Alert*\n\n"+
			"Position: `%s`\n"+
			"Symbol: `%s` (%s↓ / %s↑)\n"+
			"Reason: %s\n"+
			"Funding collected: `%s USDT`",
		p.ID, p.Symbol, p.ShortVenue, p.LongVenue,
		reason,
		p.FundingCollected.StringFixed(4),
	)

	if b.bot == nil {
		slog.Info("telegram: [paper] position alert", "position", p.ID, "reason", reason)
		return nil
	}

	keyboard := tu.InlineKeyboard(
		tu.InlineKeyboardRow(
			tu.InlineKeyboardButton("🔴 Close Position").WithCallbackData(callbackClose+":"+p.ID),
			tu.InlineKeyboardButton("👀 Keep Watching").WithCallbackData("ignore:"+p.ID),
		),
	)

	_, err := b.bot.SendMessage(ctx, &telego.SendMessageParams{
		ChatID:      tu.ID(b.chatID),
		Text:        text,
		ParseMode:   "Markdown",
		ReplyMarkup: keyboard,
	})
	return err
}

// SendText sends a plain informational message.
func (b *Bot) SendText(ctx context.Context, text string) error {
	if b.bot == nil {
		slog.Info("telegram: [paper] message", "text", text)
		return nil
	}
	_, err := b.bot.SendMessage(ctx, &telego.SendMessageParams{
		ChatID: tu.ID(b.chatID),
		Text:   text,
	})
	return err
}
