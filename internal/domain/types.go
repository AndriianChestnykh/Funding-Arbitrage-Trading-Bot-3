package domain

import (
	"time"

	"github.com/shopspring/decimal"
)

type Side string

const (
	SideLong  Side = "long"
	SideShort Side = "short"
)

type OrderType string

const (
	OrderTypeMarket OrderType = "market"
	OrderTypeLimit  OrderType = "limit"
)

type PositionStatus string

const (
	StatusOpen   PositionStatus = "open"
	StatusClosed PositionStatus = "closed"
)

// Tick is a normalized funding tick from any venue.
type Tick struct {
	Venue     string
	Symbol    string          // e.g. "BTCUSDT"
	Rate      decimal.Decimal // funding rate per period, e.g. 0.0005 = +0.05%
	NextAt    time.Time       // next funding settlement time
	MarkPrice decimal.Decimal
	Received  time.Time
}

// TimeToFunding returns the duration until the next funding settlement.
func (t Tick) TimeToFunding() time.Duration {
	d := time.Until(t.NextAt)
	if d < 0 {
		return 0
	}
	return d
}

// Candidate is an opportunity identified by the engine.
type Candidate struct {
	ShortVenue string
	LongVenue  string
	Symbol     string
	ShortRate  decimal.Decimal // funding rate on the short venue (positive = longs pay shorts)
	LongRate   decimal.Decimal // funding rate on the long venue (negative = shorts pay longs)
	NetEdge    decimal.Decimal // net funding earned per period after estimated costs
	TTF        time.Duration   // time to the nearest funding settlement
}

// AnnualizedEdgePct returns the gross annualized edge as a percentage.
func (c Candidate) AnnualizedEdgePct() decimal.Decimal {
	if c.TTF <= 0 {
		return decimal.Zero
	}
	periodsPerYear := decimal.NewFromFloat(float64(365*24*time.Hour) / float64(c.TTF))
	return c.NetEdge.Mul(periodsPerYear).Mul(decimal.NewFromInt(100))
}

// Proposal is sent to Telegram for user approval.
type Proposal struct {
	ID        string
	Candidate Candidate
	CreatedAt time.Time
	ExpiresAt time.Time
}

// Approval is the user's response to a Proposal.
type Approval struct {
	ProposalID string
	Approved   bool
	Snoozed    bool
	SnoozedFor time.Duration
}

// Order is a request to place an order on a venue.
type Order struct {
	ClientOrderID string
	Venue         string
	Symbol        string
	Side          Side
	Size          decimal.Decimal
	Type          OrderType
	LimitPrice    decimal.Decimal // only for limit orders
}

// Fill is the confirmed result of a placed order.
type Fill struct {
	OrderID  string
	Venue    string
	Symbol   string
	Side     Side
	Size     decimal.Decimal
	Price    decimal.Decimal
	Fee      decimal.Decimal
	FilledAt time.Time
}

// Position is an open arbitrage position.
type Position struct {
	ID               string
	Symbol           string
	ShortVenue       string
	LongVenue        string
	Size             decimal.Decimal
	EntryShortRate   decimal.Decimal
	EntryLongRate    decimal.Decimal
	OpenedAt         time.Time
	FundingCollected decimal.Decimal
	Status           PositionStatus
	LastCheckedAt    time.Time
}
