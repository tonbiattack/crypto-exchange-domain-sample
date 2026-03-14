package testdb

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type testHelper interface {
	Helper()
	Fatalf(format string, args ...any)
}

type MasterData struct {
	ActiveUserStatusID        int64
	FrozenUserStatusID        int64
	PendingDepositStatusID    int64
	CompletedDepositStatusID  int64
	FailedDepositStatusID     int64
	PendingWithdrawalID       int64
	CompletedWithdrawalID     int64
	FailedWithdrawalID        int64
	FilledOrderStatusID       int64
	OpenOrderStatusID         int64
	SystemActorTypeID         int64
	AdminActorTypeID          int64
	AutoCaseSourceTypeID      int64
	OpenCaseStatusID          int64
	InvestigatingCaseStatusID int64
	ClosedCaseStatusID        int64
	HighRiskLevelID           int64
	CriticalRiskLevelID       int64
	OpenAlertStatusID         int64
	ReviewedAlertStatusID     int64
	FreezeActionTypeID        int64
	FrozenEventTypeID         int64
	JPYCurrencyID             int64
	BTCurrencyID              int64
	ETHCurrencyID             int64
	XRPCurrencyID             int64
	OccupationOtherID         int64
	IncomeMidID               int64
	AssetMidID                int64
}

func LoadMasterData(ctx context.Context, tx *sql.Tx) MasterData {
	return MasterData{
		ActiveUserStatusID:        mustLookupID(ctx, tx, "SELECT id FROM user_statuses WHERE value = 'ACTIVE' LIMIT 1"),
		FrozenUserStatusID:        mustLookupID(ctx, tx, "SELECT id FROM user_statuses WHERE value = 'FROZEN' LIMIT 1"),
		PendingDepositStatusID:    mustLookupID(ctx, tx, "SELECT id FROM deposit_statuses WHERE value = 'PENDING' LIMIT 1"),
		CompletedDepositStatusID:  mustLookupID(ctx, tx, "SELECT id FROM deposit_statuses WHERE value = 'COMPLETED' LIMIT 1"),
		FailedDepositStatusID:     mustLookupID(ctx, tx, "SELECT id FROM deposit_statuses WHERE value = 'FAILED' LIMIT 1"),
		PendingWithdrawalID:       mustLookupID(ctx, tx, "SELECT id FROM withdrawal_statuses WHERE value = 'PENDING' LIMIT 1"),
		CompletedWithdrawalID:     mustLookupID(ctx, tx, "SELECT id FROM withdrawal_statuses WHERE value = 'COMPLETED' LIMIT 1"),
		FailedWithdrawalID:        mustLookupID(ctx, tx, "SELECT id FROM withdrawal_statuses WHERE value = 'FAILED' LIMIT 1"),
		FilledOrderStatusID:       mustLookupID(ctx, tx, "SELECT id FROM order_statuses WHERE value = 'FILLED' LIMIT 1"),
		OpenOrderStatusID:         mustLookupID(ctx, tx, "SELECT id FROM order_statuses WHERE value = 'OPEN' LIMIT 1"),
		SystemActorTypeID:         mustLookupID(ctx, tx, "SELECT id FROM actor_types WHERE value = 'SYSTEM' LIMIT 1"),
		AdminActorTypeID:          mustLookupID(ctx, tx, "SELECT id FROM actor_types WHERE value = 'ADMIN' LIMIT 1"),
		AutoCaseSourceTypeID:      mustLookupID(ctx, tx, "SELECT id FROM case_source_types WHERE value = 'AUTO' LIMIT 1"),
		OpenCaseStatusID:          mustLookupID(ctx, tx, "SELECT id FROM case_statuses WHERE value = 'OPEN' LIMIT 1"),
		InvestigatingCaseStatusID: mustLookupID(ctx, tx, "SELECT id FROM case_statuses WHERE value = 'INVESTIGATING' LIMIT 1"),
		ClosedCaseStatusID:        mustLookupID(ctx, tx, "SELECT id FROM case_statuses WHERE value = 'CLOSED' LIMIT 1"),
		HighRiskLevelID:           mustLookupID(ctx, tx, "SELECT id FROM risk_levels WHERE value = 'HIGH' LIMIT 1"),
		CriticalRiskLevelID:       mustLookupID(ctx, tx, "SELECT id FROM risk_levels WHERE value = 'CRITICAL' LIMIT 1"),
		OpenAlertStatusID:         mustLookupID(ctx, tx, "SELECT id FROM alert_event_statuses WHERE value = 'OPEN' LIMIT 1"),
		ReviewedAlertStatusID:     mustLookupID(ctx, tx, "SELECT id FROM alert_event_statuses WHERE value = 'REVIEWED' LIMIT 1"),
		FreezeActionTypeID:        mustLookupID(ctx, tx, "SELECT id FROM account_action_types WHERE value = 'FREEZE' LIMIT 1"),
		FrozenEventTypeID:         mustLookupID(ctx, tx, "SELECT id FROM user_status_event_types WHERE value = 'FROZEN' LIMIT 1"),
		JPYCurrencyID:             mustLookupID(ctx, tx, "SELECT id FROM currencies WHERE code = 'JPY' LIMIT 1"),
		BTCurrencyID:              mustLookupID(ctx, tx, "SELECT id FROM currencies WHERE code = 'BTC' LIMIT 1"),
		ETHCurrencyID:             mustLookupID(ctx, tx, "SELECT id FROM currencies WHERE code = 'ETH' LIMIT 1"),
		XRPCurrencyID:             mustLookupID(ctx, tx, "SELECT id FROM currencies WHERE code = 'XRP' LIMIT 1"),
		OccupationOtherID:         mustLookupID(ctx, tx, "SELECT id FROM occupations WHERE value = 'OTHER' LIMIT 1"),
		IncomeMidID:               mustLookupID(ctx, tx, "SELECT id FROM annual_income_brackets WHERE value = '5M_TO_7M' LIMIT 1"),
		AssetMidID:                mustLookupID(ctx, tx, "SELECT id FROM financial_asset_brackets WHERE value = '5M_TO_10M' LIMIT 1"),
	}
}

func LoadMasterDataForTest(t testHelper, ctx context.Context, tx *sql.Tx) (masters MasterData) {
	t.Helper()

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("マスタデータの読み込みに失敗しました: %v", recovered)
		}
	}()

	return LoadMasterData(ctx, tx)
}

type UserBuilder struct {
	PublicUserHash  string
	MemberCode      string
	CurrentStatusID int64
	RegisteredAt    time.Time
}

type UserProfileVersionBuilder struct {
	UserID                  int64
	VersionNo               int64
	LastName                string
	FirstName               string
	BirthDate               time.Time
	CountryCode             string
	OccupationID            int64
	AnnualIncomeBracketID   int64
	FinancialAssetBracketID int64
	DeclaredAt              time.Time
	ChangeReason            string
}

func NewUserProfileVersionBuilder() *UserProfileVersionBuilder {
	now := time.Now().UTC()
	return &UserProfileVersionBuilder{
		VersionNo:    1,
		LastName:     "Integration",
		FirstName:    "Tester",
		BirthDate:    time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC),
		CountryCode:  "JP",
		DeclaredAt:   now,
		ChangeReason: "integration test profile snapshot",
	}
}

func (b *UserProfileVersionBuilder) Build(ctx context.Context, tx *sql.Tx) int64 {
	result, err := tx.ExecContext(
		ctx,
		`INSERT INTO user_profile_versions (
			user_id, version_no, last_name, first_name, birth_date, country_code,
			occupation_id, annual_income_bracket_id, financial_asset_bracket_id, declared_at, change_reason
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		b.UserID,
		b.VersionNo,
		b.LastName,
		b.FirstName,
		b.BirthDate,
		b.CountryCode,
		b.OccupationID,
		b.AnnualIncomeBracketID,
		b.FinancialAssetBracketID,
		b.DeclaredAt,
		b.ChangeReason,
	)
	if err != nil {
		panic(fmt.Sprintf("ユーザープロファイル履歴のテストデータ作成に失敗しました: %v", err))
	}

	id, err := result.LastInsertId()
	if err != nil {
		panic(fmt.Sprintf("ユーザープロファイル履歴IDの取得に失敗しました: %v", err))
	}

	return id
}

func (b *UserProfileVersionBuilder) BuildForTest(t testHelper, ctx context.Context, tx *sql.Tx) (id int64) {
	t.Helper()

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("%v", recovered)
		}
	}()

	return b.Build(ctx, tx)
}

func NewUserBuilder() *UserBuilder {
	now := time.Now().UTC()
	return &UserBuilder{
		PublicUserHash: fmt.Sprintf("test-user-%d", now.UnixNano()),
		MemberCode:     fmt.Sprintf("T%010d", now.UnixNano()%1_000_000_0000),
		RegisteredAt:   now,
	}
}

func (b *UserBuilder) WithStatusID(id int64) *UserBuilder {
	b.CurrentStatusID = id
	return b
}

func (b *UserBuilder) Build(ctx context.Context, tx *sql.Tx) int64 {
	result, err := tx.ExecContext(
		ctx,
		`INSERT INTO users (public_user_hash, member_code, current_status_id, registered_at) VALUES (?, ?, ?, ?)`,
		b.PublicUserHash,
		b.MemberCode,
		b.CurrentStatusID,
		b.RegisteredAt,
	)
	if err != nil {
		panic(fmt.Sprintf("ユーザーのテストデータ作成に失敗しました: %v", err))
	}

	id, err := result.LastInsertId()
	if err != nil {
		panic(fmt.Sprintf("ユーザーIDの取得に失敗しました: %v", err))
	}

	return id
}

func (b *UserBuilder) BuildForTest(t testHelper, ctx context.Context, tx *sql.Tx) (id int64) {
	t.Helper()

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("%v", recovered)
		}
	}()

	return b.Build(ctx, tx)
}

type FiatDepositBuilder struct {
	UserID          int64
	PublicHash      string
	CurrencyID      int64
	Amount          string
	DepositStatusID int64
	RequestedAt     time.Time
	CompletedAt     sql.NullTime
	FailedAt        sql.NullTime
}

func NewFiatDepositBuilder() *FiatDepositBuilder {
	now := time.Now().UTC()
	return &FiatDepositBuilder{
		PublicHash:  fmt.Sprintf("test-fiat-deposit-%d", now.UnixNano()),
		Amount:      "1000000",
		RequestedAt: now,
		CompletedAt: sql.NullTime{Time: now.Add(5 * time.Minute), Valid: true},
	}
}

func (b *FiatDepositBuilder) Build(ctx context.Context, tx *sql.Tx) int64 {
	result, err := tx.ExecContext(
		ctx,
		`INSERT INTO fiat_deposits (
			user_id, public_deposit_hash, currency_id, amount, deposit_status_id, requested_at, completed_at, failed_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		b.UserID,
		b.PublicHash,
		b.CurrencyID,
		b.Amount,
		b.DepositStatusID,
		b.RequestedAt,
		b.CompletedAt,
		b.FailedAt,
	)
	if err != nil {
		panic(fmt.Sprintf("法定入金のテストデータ作成に失敗しました: %v", err))
	}

	id, err := result.LastInsertId()
	if err != nil {
		panic(fmt.Sprintf("法定入金IDの取得に失敗しました: %v", err))
	}

	return id
}

func (b *FiatDepositBuilder) BuildForTest(t testHelper, ctx context.Context, tx *sql.Tx) (id int64) {
	t.Helper()

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("%v", recovered)
		}
	}()

	return b.Build(ctx, tx)
}

type FiatWithdrawalBuilder struct {
	UserID             int64
	PublicHash         string
	CurrencyID         int64
	Amount             string
	WithdrawalStatusID int64
	RequestedAt        time.Time
	CompletedAt        sql.NullTime
	FailedAt           sql.NullTime
}

func NewFiatWithdrawalBuilder() *FiatWithdrawalBuilder {
	now := time.Now().UTC()
	return &FiatWithdrawalBuilder{
		PublicHash:  fmt.Sprintf("test-fiat-withdrawal-%d", now.UnixNano()),
		Amount:      "900000",
		RequestedAt: now,
		CompletedAt: sql.NullTime{Time: now.Add(20 * time.Minute), Valid: true},
	}
}

func (b *FiatWithdrawalBuilder) Build(ctx context.Context, tx *sql.Tx) int64 {
	result, err := tx.ExecContext(
		ctx,
		`INSERT INTO fiat_withdrawals (
			user_id, public_withdrawal_hash, currency_id, amount, withdrawal_status_id, requested_at, completed_at, failed_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		b.UserID,
		b.PublicHash,
		b.CurrencyID,
		b.Amount,
		b.WithdrawalStatusID,
		b.RequestedAt,
		b.CompletedAt,
		b.FailedAt,
	)
	if err != nil {
		panic(fmt.Sprintf("法定出金のテストデータ作成に失敗しました: %v", err))
	}

	id, err := result.LastInsertId()
	if err != nil {
		panic(fmt.Sprintf("法定出金IDの取得に失敗しました: %v", err))
	}

	return id
}

func (b *FiatWithdrawalBuilder) BuildForTest(t testHelper, ctx context.Context, tx *sql.Tx) (id int64) {
	t.Helper()

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("%v", recovered)
		}
	}()

	return b.Build(ctx, tx)
}

type CryptoDepositBuilder struct {
	UserID          int64
	PublicHash      string
	CurrencyID      int64
	TxHash          string
	Amount          string
	DepositStatusID int64
	DetectedAt      time.Time
	ConfirmedAt     sql.NullTime
	FailedAt        sql.NullTime
}

func NewCryptoDepositBuilder() *CryptoDepositBuilder {
	now := time.Now().UTC()
	return &CryptoDepositBuilder{
		PublicHash: fmt.Sprintf("test-crypto-deposit-%d", now.UnixNano()),
		TxHash:     fmt.Sprintf("test-crypto-deposit-tx-%d", now.UnixNano()),
		Amount:     "1.25",
		DetectedAt: now,
		ConfirmedAt: sql.NullTime{
			Time:  now.Add(30 * time.Minute),
			Valid: true,
		},
	}
}

func (b *CryptoDepositBuilder) Build(ctx context.Context, tx *sql.Tx) int64 {
	result, err := tx.ExecContext(
		ctx,
		`INSERT INTO crypto_deposits (
			user_id, public_deposit_hash, currency_id, tx_hash, amount, deposit_status_id, detected_at, confirmed_at, failed_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		b.UserID,
		b.PublicHash,
		b.CurrencyID,
		b.TxHash,
		b.Amount,
		b.DepositStatusID,
		b.DetectedAt,
		b.ConfirmedAt,
		b.FailedAt,
	)
	if err != nil {
		panic(fmt.Sprintf("暗号資産入金のテストデータ作成に失敗しました: %v", err))
	}

	id, err := result.LastInsertId()
	if err != nil {
		panic(fmt.Sprintf("暗号資産入金IDの取得に失敗しました: %v", err))
	}

	return id
}

func (b *CryptoDepositBuilder) BuildForTest(t testHelper, ctx context.Context, tx *sql.Tx) (id int64) {
	t.Helper()

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("%v", recovered)
		}
	}()

	return b.Build(ctx, tx)
}

type CryptoWithdrawalBuilder struct {
	UserID             int64
	PublicHash         string
	CurrencyID         int64
	DestinationAddress string
	Amount             string
	TxHash             sql.NullString
	WithdrawalStatusID int64
	RequestedAt        time.Time
	CompletedAt        sql.NullTime
	FailedAt           sql.NullTime
}

func NewCryptoWithdrawalBuilder() *CryptoWithdrawalBuilder {
	now := time.Now().UTC()
	return &CryptoWithdrawalBuilder{
		PublicHash:         fmt.Sprintf("test-crypto-withdrawal-%d", now.UnixNano()),
		DestinationAddress: fmt.Sprintf("test-address-%d", now.UnixNano()),
		Amount:             "1.10",
		TxHash: sql.NullString{
			String: fmt.Sprintf("test-crypto-withdrawal-tx-%d", now.UnixNano()),
			Valid:  true,
		},
		RequestedAt: now,
		CompletedAt: sql.NullTime{
			Time:  now.Add(20 * time.Minute),
			Valid: true,
		},
	}
}

func (b *CryptoWithdrawalBuilder) Build(ctx context.Context, tx *sql.Tx) int64 {
	result, err := tx.ExecContext(
		ctx,
		`INSERT INTO crypto_withdrawals (
			user_id, public_withdrawal_hash, currency_id, destination_address, amount, tx_hash, withdrawal_status_id, requested_at, completed_at, failed_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		b.UserID,
		b.PublicHash,
		b.CurrencyID,
		b.DestinationAddress,
		b.Amount,
		b.TxHash,
		b.WithdrawalStatusID,
		b.RequestedAt,
		b.CompletedAt,
		b.FailedAt,
	)
	if err != nil {
		panic(fmt.Sprintf("暗号資産出金のテストデータ作成に失敗しました: %v", err))
	}

	id, err := result.LastInsertId()
	if err != nil {
		panic(fmt.Sprintf("暗号資産出金IDの取得に失敗しました: %v", err))
	}

	return id
}

func (b *CryptoWithdrawalBuilder) BuildForTest(t testHelper, ctx context.Context, tx *sql.Tx) (id int64) {
	t.Helper()

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("%v", recovered)
		}
	}()

	return b.Build(ctx, tx)
}

type TradingOrderBuilder struct {
	UserID         int64
	PublicHash     string
	Side           string
	OrderType      string
	FromCurrencyID int64
	ToCurrencyID   int64
	Price          string
	Quantity       string
	OrderStatusID  int64
	PlacedAt       time.Time
	CancelledAt    sql.NullTime
}

func NewTradingOrderBuilder() *TradingOrderBuilder {
	now := time.Now().UTC()
	return &TradingOrderBuilder{
		PublicHash: fmt.Sprintf("test-order-%d", now.UnixNano()),
		Side:       "BUY",
		OrderType:  "LIMIT",
		Price:      "123.45",
		Quantity:   "4.00",
		PlacedAt:   now,
	}
}

func (b *TradingOrderBuilder) Build(ctx context.Context, tx *sql.Tx) int64 {
	result, err := tx.ExecContext(
		ctx,
		`INSERT INTO trading_orders (
			user_id, public_order_hash, side, order_type, from_currency_id, to_currency_id, price, quantity, order_status_id, placed_at, cancelled_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		b.UserID,
		b.PublicHash,
		b.Side,
		b.OrderType,
		b.FromCurrencyID,
		b.ToCurrencyID,
		b.Price,
		b.Quantity,
		b.OrderStatusID,
		b.PlacedAt,
		b.CancelledAt,
	)
	if err != nil {
		panic(fmt.Sprintf("取引注文のテストデータ作成に失敗しました: %v", err))
	}

	id, err := result.LastInsertId()
	if err != nil {
		panic(fmt.Sprintf("取引注文IDの取得に失敗しました: %v", err))
	}

	return id
}

func (b *TradingOrderBuilder) BuildForTest(t testHelper, ctx context.Context, tx *sql.Tx) (id int64) {
	t.Helper()

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("%v", recovered)
		}
	}()

	return b.Build(ctx, tx)
}

type TradeExecutionBuilder struct {
	OrderID          int64
	UserID           int64
	PublicHash       string
	FromCurrencyID   int64
	ToCurrencyID     int64
	ExecutedPrice    string
	ExecutedQuantity string
	FromAmount       string
	ToAmount         string
	FeeCurrencyID    int64
	FeeAmount        string
	ExecutedAt       time.Time
}

func NewTradeExecutionBuilder() *TradeExecutionBuilder {
	now := time.Now().UTC()
	return &TradeExecutionBuilder{
		PublicHash:       fmt.Sprintf("test-execution-%d", now.UnixNano()),
		ExecutedPrice:    "123.45",
		ExecutedQuantity: "4.00",
		FromAmount:       "493.80",
		ToAmount:         "4.00",
		FeeAmount:        "0.01",
		ExecutedAt:       now,
	}
}

func (b *TradeExecutionBuilder) Build(ctx context.Context, tx *sql.Tx) int64 {
	result, err := tx.ExecContext(
		ctx,
		`INSERT INTO trade_executions (
			order_id, user_id, public_execution_hash, from_currency_id, to_currency_id, executed_price, executed_quantity, from_amount, to_amount, fee_currency_id, fee_amount, executed_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE(NULLIF(?, ''), (? * ?)), COALESCE(NULLIF(?, ''), ?), ?, ?, ?)`,
		b.OrderID,
		b.UserID,
		b.PublicHash,
		b.FromCurrencyID,
		b.ToCurrencyID,
		b.ExecutedPrice,
		b.ExecutedQuantity,
		b.FromAmount,
		b.ExecutedPrice,
		b.ExecutedQuantity,
		b.ToAmount,
		b.ExecutedQuantity,
		b.FeeCurrencyID,
		b.FeeAmount,
		b.ExecutedAt,
	)
	if err != nil {
		panic(fmt.Sprintf("約定のテストデータ作成に失敗しました: %v", err))
	}

	id, err := result.LastInsertId()
	if err != nil {
		panic(fmt.Sprintf("約定IDの取得に失敗しました: %v", err))
	}

	return id
}

func (b *TradeExecutionBuilder) BuildForTest(t testHelper, ctx context.Context, tx *sql.Tx) (id int64) {
	t.Helper()

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("%v", recovered)
		}
	}()

	return b.Build(ctx, tx)
}

type AlertRuleBuilder struct {
	PublicRuleHash string
	RuleName       string
	RuleType       string
	Severity       string
	ThresholdJSON  string
}

func NewAlertRuleBuilder() *AlertRuleBuilder {
	now := time.Now().UTC()
	return &AlertRuleBuilder{
		PublicRuleHash: fmt.Sprintf("%064d", now.UnixNano()),
		RuleName:       fmt.Sprintf("Test Rule %d", now.UnixNano()),
		RuleType:       "RAPID_OUTFLOW",
		Severity:       "CRITICAL",
		ThresholdJSON:  `{"window_hours":24,"outflow_ratio":0.8}`,
	}
}

func (b *AlertRuleBuilder) Build(ctx context.Context, tx *sql.Tx) int64 {
	result, err := tx.ExecContext(
		ctx,
		`INSERT INTO alert_rules (public_rule_hash, rule_name, rule_type, severity, threshold_json, is_active)
		 VALUES (?, ?, ?, ?, ?, 1)`,
		b.PublicRuleHash,
		b.RuleName,
		b.RuleType,
		b.Severity,
		b.ThresholdJSON,
	)
	if err != nil {
		panic(fmt.Sprintf("アラートルールのテストデータ作成に失敗しました: %v", err))
	}

	id, err := result.LastInsertId()
	if err != nil {
		panic(fmt.Sprintf("アラートルールIDの取得に失敗しました: %v", err))
	}

	return id
}

func (b *AlertRuleBuilder) BuildForTest(t testHelper, ctx context.Context, tx *sql.Tx) (id int64) {
	t.Helper()

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("%v", recovered)
		}
	}()

	return b.Build(ctx, tx)
}

type AlertEventLogBuilder struct {
	UserID             int64
	RuleID             int64
	AlertEventStatusID int64
	FiatWithdrawalID   int64
	TradeExecutionID   sql.NullInt64
	Score              string
	DetectedAt         time.Time
	Note               string
}

func NewAlertEventLogBuilder() *AlertEventLogBuilder {
	now := time.Now().UTC()
	return &AlertEventLogBuilder{
		Score:      "99.1000",
		DetectedAt: now,
		Note:       fmt.Sprintf("test-alert-%d", now.UnixNano()),
	}
}

func (b *AlertEventLogBuilder) Build(ctx context.Context, tx *sql.Tx) int64 {
	result, err := tx.ExecContext(
		ctx,
		`INSERT INTO alert_event_logs (
			user_id, rule_id, alert_event_status_id, trade_execution_id, fiat_deposit_id,
			fiat_withdrawal_id, crypto_deposit_id, crypto_withdrawal_id, score, detected_at, note
		) VALUES (?, ?, ?, ?, NULL, ?, NULL, NULL, ?, ?, ?)`,
		b.UserID,
		b.RuleID,
		b.AlertEventStatusID,
		b.TradeExecutionID,
		b.FiatWithdrawalID,
		b.Score,
		b.DetectedAt,
		b.Note,
	)
	if err != nil {
		panic(fmt.Sprintf("アラートイベントのテストデータ作成に失敗しました: %v", err))
	}

	id, err := result.LastInsertId()
	if err != nil {
		panic(fmt.Sprintf("アラートイベントIDの取得に失敗しました: %v", err))
	}

	return id
}

func (b *AlertEventLogBuilder) BuildForTest(t testHelper, ctx context.Context, tx *sql.Tx) (id int64) {
	t.Helper()

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("%v", recovered)
		}
	}()

	return b.Build(ctx, tx)
}

type SuspiciousCaseBuilder struct {
	UserID          int64
	PublicCaseHash  string
	OpenedByTypeID  int64
	OpenedByID      string
	SourceTypeID    int64
	AlertEventLogID sql.NullInt64
	Title           string
	CurrentStatusID int64
	RiskLevelID     int64
	AssignedTo      sql.NullString
	OpenedAt        time.Time
	ClosedAt        sql.NullTime
	ClosedReason    sql.NullString
	Disposition     sql.NullString
}

func NewSuspiciousCaseBuilder() *SuspiciousCaseBuilder {
	now := time.Now().UTC()
	return &SuspiciousCaseBuilder{
		PublicCaseHash: fmt.Sprintf("%064d", now.UnixNano()),
		OpenedByID:     "integration-test",
		Title:          "Integration test suspicious case",
		OpenedAt:       now,
	}
}

func (b *SuspiciousCaseBuilder) Build(ctx context.Context, tx *sql.Tx) int64 {
	result, err := tx.ExecContext(
		ctx,
		`INSERT INTO suspicious_cases (
			user_id, public_case_hash, opened_by_type_id, opened_by_id, source_type_id,
			alert_event_log_id, title, current_status_id, risk_level_id, assigned_to, opened_at, closed_at, closed_reason, disposition
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		b.UserID,
		b.PublicCaseHash,
		b.OpenedByTypeID,
		b.OpenedByID,
		b.SourceTypeID,
		b.AlertEventLogID,
		b.Title,
		b.CurrentStatusID,
		b.RiskLevelID,
		b.AssignedTo,
		b.OpenedAt,
		b.ClosedAt,
		b.ClosedReason,
		b.Disposition,
	)
	if err != nil {
		panic(fmt.Sprintf("疑わしいケースのテストデータ作成に失敗しました: %v", err))
	}

	id, err := result.LastInsertId()
	if err != nil {
		panic(fmt.Sprintf("疑わしいケースIDの取得に失敗しました: %v", err))
	}

	return id
}

func (b *SuspiciousCaseBuilder) BuildForTest(t testHelper, ctx context.Context, tx *sql.Tx) (id int64) {
	t.Helper()

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("%v", recovered)
		}
	}()

	return b.Build(ctx, tx)
}

type UserStatusChangeEventBuilder struct {
	UserID      int64
	EventTypeID int64
	ActorTypeID int64
	ActorID     string
	Reason      string
	OccurredAt  time.Time
}

func NewUserStatusChangeEventBuilder() *UserStatusChangeEventBuilder {
	now := time.Now().UTC()
	return &UserStatusChangeEventBuilder{
		ActorID:    "integration-test",
		Reason:     "integration test status change",
		OccurredAt: now,
	}
}

func (b *UserStatusChangeEventBuilder) Build(ctx context.Context, tx *sql.Tx) int64 {
	result, err := tx.ExecContext(
		ctx,
		`INSERT INTO user_status_change_events (
			user_id, event_type_id, actor_type_id, actor_id, reason, occurred_at
		) VALUES (?, ?, ?, ?, ?, ?)`,
		b.UserID,
		b.EventTypeID,
		b.ActorTypeID,
		b.ActorID,
		b.Reason,
		b.OccurredAt,
	)
	if err != nil {
		panic(fmt.Sprintf("ユーザーステータス変更イベントのテストデータ作成に失敗しました: %v", err))
	}

	id, err := result.LastInsertId()
	if err != nil {
		panic(fmt.Sprintf("ユーザーステータス変更イベントIDの取得に失敗しました: %v", err))
	}

	return id
}

func (b *UserStatusChangeEventBuilder) BuildForTest(t testHelper, ctx context.Context, tx *sql.Tx) (id int64) {
	t.Helper()

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("%v", recovered)
		}
	}()

	return b.Build(ctx, tx)
}

type UserStatusHistoryBuilder struct {
	UserID              int64
	StatusChangeEventID int64
	FromStatusID        sql.NullInt64
	ToStatusID          int64
	ChangedAt           time.Time
}

func NewUserStatusHistoryBuilder() *UserStatusHistoryBuilder {
	return &UserStatusHistoryBuilder{
		ChangedAt: time.Now().UTC(),
	}
}

func (b *UserStatusHistoryBuilder) Build(ctx context.Context, tx *sql.Tx) int64 {
	result, err := tx.ExecContext(
		ctx,
		`INSERT INTO user_status_histories (
			user_id, status_change_event_id, from_status_id, to_status_id, changed_at
		) VALUES (?, ?, ?, ?, ?)`,
		b.UserID,
		b.StatusChangeEventID,
		b.FromStatusID,
		b.ToStatusID,
		b.ChangedAt,
	)
	if err != nil {
		panic(fmt.Sprintf("ユーザーステータス履歴のテストデータ作成に失敗しました: %v", err))
	}

	id, err := result.LastInsertId()
	if err != nil {
		panic(fmt.Sprintf("ユーザーステータス履歴IDの取得に失敗しました: %v", err))
	}

	return id
}

func (b *UserStatusHistoryBuilder) BuildForTest(t testHelper, ctx context.Context, tx *sql.Tx) (id int64) {
	t.Helper()

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("%v", recovered)
		}
	}()

	return b.Build(ctx, tx)
}

type AccountActionBuilder struct {
	UserID           int64
	SuspiciousCaseID sql.NullInt64
	ActionTypeID     int64
	ActorTypeID      int64
	ActorID          string
	ActionReason     string
	RequestedAt      time.Time
	CompletedAt      sql.NullTime
}

func NewAccountActionBuilder() *AccountActionBuilder {
	now := time.Now().UTC()
	return &AccountActionBuilder{
		ActorID:      "integration-test",
		ActionReason: "integration test account action",
		RequestedAt:  now,
	}
}

func (b *AccountActionBuilder) Build(ctx context.Context, tx *sql.Tx) int64 {
	result, err := tx.ExecContext(
		ctx,
		`INSERT INTO account_actions (
			user_id, suspicious_case_id, action_type_id, actor_type_id, actor_id, action_reason, requested_at, completed_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		b.UserID,
		b.SuspiciousCaseID,
		b.ActionTypeID,
		b.ActorTypeID,
		b.ActorID,
		b.ActionReason,
		b.RequestedAt,
		b.CompletedAt,
	)
	if err != nil {
		panic(fmt.Sprintf("口座措置のテストデータ作成に失敗しました: %v", err))
	}

	id, err := result.LastInsertId()
	if err != nil {
		panic(fmt.Sprintf("口座措置IDの取得に失敗しました: %v", err))
	}

	return id
}

func (b *AccountActionBuilder) BuildForTest(t testHelper, ctx context.Context, tx *sql.Tx) (id int64) {
	t.Helper()

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("%v", recovered)
		}
	}()

	return b.Build(ctx, tx)
}

func mustLookupID(ctx context.Context, tx *sql.Tx, query string) int64 {
	var id int64
	if err := tx.QueryRowContext(ctx, query).Scan(&id); err != nil {
		panic(fmt.Sprintf("マスタIDの取得に失敗しました: %v query=%s", err, query))
	}
	return id
}
