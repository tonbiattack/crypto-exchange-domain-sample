package integrationtests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"private-crypto-exchange-domain-sample/integrationtests/testdb"
)

func TestDailyActivitySummary(t *testing.T) {
	t.Run("日次アクティビティ集計_各業務件数を同一日に集約できる", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		baseDate := time.Date(2031, 1, 10, 9, 0, 0, 0, time.UTC)

		userID := testdb.NewUserBuilder().
			WithStatusID(masters.ActiveUserStatusID).
			BuildForTest(t, ctx, tx)

		orderID := (&testdb.TradingOrderBuilder{
			UserID:         userID,
			PublicHash:     "daily-activity-order",
			Side:           "BUY",
			OrderType:      "LIMIT",
			FromCurrencyID: masters.JPYCurrencyID,
			ToCurrencyID:   masters.BTCurrencyID,
			Price:          "5000000",
			Quantity:       "0.10000000",
			OrderStatusID:  masters.FilledOrderStatusID,
			PlacedAt:       baseDate,
		}).BuildForTest(t, ctx, tx)

		(&testdb.TradeExecutionBuilder{
			OrderID:          orderID,
			UserID:           userID,
			PublicHash:       "daily-activity-execution",
			FromCurrencyID:   masters.JPYCurrencyID,
			ToCurrencyID:     masters.BTCurrencyID,
			ExecutedPrice:    "5000000",
			ExecutedQuantity: "0.10000000",
			FeeCurrencyID:    masters.JPYCurrencyID,
			FeeAmount:        "1000",
			ExecutedAt:       baseDate.Add(10 * time.Minute),
		}).BuildForTest(t, ctx, tx)

		(&testdb.FiatDepositBuilder{
			UserID:          userID,
			PublicHash:      "daily-activity-fiat-deposit",
			CurrencyID:      masters.JPYCurrencyID,
			Amount:          "100000",
			DepositStatusID: masters.CompletedDepositStatusID,
			RequestedAt:     baseDate.Add(20 * time.Minute),
			CompletedAt:     sql.NullTime{Time: baseDate.Add(30 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)

		(&testdb.FiatWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "daily-activity-fiat-withdrawal",
			CurrencyID:         masters.JPYCurrencyID,
			Amount:             "50000",
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        baseDate.Add(40 * time.Minute),
			CompletedAt:        sql.NullTime{Time: baseDate.Add(50 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)

		(&testdb.CryptoDepositBuilder{
			UserID:          userID,
			PublicHash:      "daily-activity-crypto-deposit",
			CurrencyID:      masters.BTCurrencyID,
			TxHash:          "daily-activity-crypto-deposit-tx",
			Amount:          "0.25000000",
			DepositStatusID: masters.CompletedDepositStatusID,
			DetectedAt:      baseDate.Add(time.Hour),
			ConfirmedAt:     sql.NullTime{Time: baseDate.Add(90 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)

		(&testdb.CryptoWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "daily-activity-crypto-withdrawal",
			CurrencyID:         masters.BTCurrencyID,
			DestinationAddress: "daily-activity-address",
			Amount:             "0.10000000",
			TxHash:             sql.NullString{String: "daily-activity-crypto-withdrawal-tx", Valid: true},
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        baseDate.Add(2 * time.Hour),
			CompletedAt:        sql.NullTime{Time: baseDate.Add(130 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/daily_activity_summary.sql")
		defer rows.Close()

		targetDate := "2031-01-10"
		var found bool
		for rows.Next() {
			var (
				activityDate          time.Time
				orderCount            int64
				executionCount        int64
				fiatDepositCount      int64
				fiatWithdrawalCount   int64
				cryptoDepositCount    int64
				cryptoWithdrawalCount int64
			)
			if err := rows.Scan(
				&activityDate,
				&orderCount,
				&executionCount,
				&fiatDepositCount,
				&fiatWithdrawalCount,
				&cryptoDepositCount,
				&cryptoWithdrawalCount,
			); err != nil {
				t.Fatalf("日次アクティビティ集計の行読み取りに失敗しました: %v", err)
			}

			if activityDate.Format("2006-01-02") == targetDate {
				found = true
				assertEqualInt64(t, orderCount, 1, "注文件数")
				assertEqualInt64(t, executionCount, 1, "約定件数")
				assertEqualInt64(t, fiatDepositCount, 1, "法定入金件数")
				assertEqualInt64(t, fiatWithdrawalCount, 1, "法定出金件数")
				assertEqualInt64(t, cryptoDepositCount, 1, "暗号資産入金件数")
				assertEqualInt64(t, cryptoWithdrawalCount, 1, "暗号資産出金件数")
			}
		}

		if err := rows.Err(); err != nil {
			t.Fatalf("日次アクティビティ集計の走査中に失敗しました: %v", err)
		}

		if !found {
			t.Fatalf("日次アクティビティ集計に対象日 %s の行が見つかりませんでした", targetDate)
		}
	})
}

func TestFailureRateSummary(t *testing.T) {
	t.Run("失敗率集計_業務種別ごとの失敗率を日別に返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)

		userID := testdb.NewUserBuilder().
			WithStatusID(masters.ActiveUserStatusID).
			BuildForTest(t, ctx, tx)

		insertFailureRateSeedData(t, ctx, tx, masters, userID)

		rows := queryRows(t, ctx, tx, "examples/failure_rate_summary.sql")
		defer rows.Close()

		expectations := map[string]struct {
			Date        string
			TotalCount  int64
			FailedCount int64
			FailureRate string
		}{
			"FIAT_DEPOSIT":      {Date: "2031-01-11", TotalCount: 2, FailedCount: 1, FailureRate: "50.00"},
			"FIAT_WITHDRAWAL":   {Date: "2031-01-12", TotalCount: 2, FailedCount: 1, FailureRate: "50.00"},
			"CRYPTO_DEPOSIT":    {Date: "2031-01-13", TotalCount: 2, FailedCount: 1, FailureRate: "50.00"},
			"CRYPTO_WITHDRAWAL": {Date: "2031-01-14", TotalCount: 2, FailedCount: 1, FailureRate: "50.00"},
		}

		foundCount := 0
		for rows.Next() {
			var (
				operationType string
				activityDate  time.Time
				totalCount    int64
				failedCount   int64
				failureRate   string
			)
			if err := rows.Scan(&operationType, &activityDate, &totalCount, &failedCount, &failureRate); err != nil {
				t.Fatalf("失敗率集計の行読み取りに失敗しました: %v", err)
			}

			expectation, ok := expectations[operationType]
			if !ok || activityDate.Format("2006-01-02") != expectation.Date {
				continue
			}

			foundCount++
			assertEqualInt64(t, totalCount, expectation.TotalCount, operationType+" の総件数")
			assertEqualInt64(t, failedCount, expectation.FailedCount, operationType+" の失敗件数")
			if failureRate != expectation.FailureRate {
				t.Fatalf("%s の失敗率が期待値と異なります: expected=%s actual=%s", operationType, expectation.FailureRate, failureRate)
			}
		}

		if err := rows.Err(); err != nil {
			t.Fatalf("失敗率集計の走査中に失敗しました: %v", err)
		}

		if foundCount != len(expectations) {
			t.Fatalf("失敗率集計で期待した行数が見つかりませんでした: expected=%d actual=%d", len(expectations), foundCount)
		}
	})
}

func TestPairVolumeSummary(t *testing.T) {
	t.Run("通貨ペア出来高集計_ペア別の約定件数と出来高を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		baseDate := time.Date(2031, 2, 1, 8, 0, 0, 0, time.UTC)

		userID := testdb.NewUserBuilder().
			WithStatusID(masters.ActiveUserStatusID).
			BuildForTest(t, ctx, tx)

		orderID := (&testdb.TradingOrderBuilder{
			UserID:         userID,
			PublicHash:     "pair-volume-order",
			Side:           "BUY",
			OrderType:      "LIMIT",
			FromCurrencyID: masters.JPYCurrencyID,
			ToCurrencyID:   masters.BTCurrencyID,
			Price:          "4500000",
			Quantity:       "0.20000000",
			OrderStatusID:  masters.FilledOrderStatusID,
			PlacedAt:       baseDate,
		}).BuildForTest(t, ctx, tx)

		(&testdb.TradeExecutionBuilder{
			OrderID:          orderID,
			UserID:           userID,
			PublicHash:       "pair-volume-execution",
			FromCurrencyID:   masters.JPYCurrencyID,
			ToCurrencyID:     masters.BTCurrencyID,
			ExecutedPrice:    "4500000",
			ExecutedQuantity: "0.20000000",
			FeeCurrencyID:    masters.JPYCurrencyID,
			FeeAmount:        "500",
			ExecutedAt:       baseDate.Add(5 * time.Minute),
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/pair_volume_summary.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				tradedDate     time.Time
				fromCurrency   string
				toCurrency     string
				executionCount int64
				baseVolume     string
				notionalVolume string
			)
			if err := rows.Scan(&tradedDate, &fromCurrency, &toCurrency, &executionCount, &baseVolume, &notionalVolume); err != nil {
				t.Fatalf("通貨ペア出来高集計の行読み取りに失敗しました: %v", err)
			}

			if tradedDate.Format("2006-01-02") == "2031-02-01" && fromCurrency == "JPY" && toCurrency == "BTC" {
				found = true
				assertEqualInt64(t, executionCount, 1, "通貨ペアの約定件数")
				if baseVolume != "0.200000000000000000" {
					t.Fatalf("通貨ペアの約定数量が期待値と異なります: expected=0.200000000000000000 actual=%s", baseVolume)
				}
				if notionalVolume != "900000.000000000000000000000000000000" {
					t.Fatalf("通貨ペアの名目出来高が期待値と異なります: expected=900000.000000000000000000000000000000 actual=%s", notionalVolume)
				}
			}
		}

		if err := rows.Err(); err != nil {
			t.Fatalf("通貨ペア出来高集計の走査中に失敗しました: %v", err)
		}

		if !found {
			t.Fatal("通貨ペア出来高集計に挿入した約定の行が見つかりませんでした")
		}
	})
}

func TestUserTradingRanking(t *testing.T) {
	t.Run("ユーザー取引ランキング_大口約定ユーザーを上位に返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		baseDate := time.Date(2031, 2, 2, 8, 0, 0, 0, time.UTC)

		userBuilder := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID)
		memberCode := userBuilder.MemberCode
		userID := userBuilder.BuildForTest(t, ctx, tx)

		orderID := (&testdb.TradingOrderBuilder{
			UserID:         userID,
			PublicHash:     "ranking-order",
			Side:           "BUY",
			OrderType:      "LIMIT",
			FromCurrencyID: masters.JPYCurrencyID,
			ToCurrencyID:   masters.BTCurrencyID,
			Price:          "1000000",
			Quantity:       "100.00000000",
			OrderStatusID:  masters.FilledOrderStatusID,
			PlacedAt:       baseDate,
		}).BuildForTest(t, ctx, tx)

		(&testdb.TradeExecutionBuilder{
			OrderID:          orderID,
			UserID:           userID,
			PublicHash:       "ranking-execution",
			FromCurrencyID:   masters.JPYCurrencyID,
			ToCurrencyID:     masters.BTCurrencyID,
			ExecutedPrice:    "1000000",
			ExecutedQuantity: "100.00000000",
			FeeCurrencyID:    masters.JPYCurrencyID,
			FeeAmount:        "1000",
			ExecutedAt:       baseDate.Add(2 * time.Minute),
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/user_trading_ranking.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				rankNo              int64
				userIDGot           int64
				memberCodeGot       string
				executionCount      int64
				totalBaseVolume     string
				totalNotionalVolume string
				firstExecutedAt     time.Time
				lastExecutedAt      time.Time
			)
			if err := rows.Scan(
				&rankNo,
				&userIDGot,
				&memberCodeGot,
				&executionCount,
				&totalBaseVolume,
				&totalNotionalVolume,
				&firstExecutedAt,
				&lastExecutedAt,
			); err != nil {
				t.Fatalf("ユーザー取引ランキングの行読み取りに失敗しました: %v", err)
			}

			if userIDGot == userID {
				found = true
				if memberCodeGot != memberCode {
					t.Fatalf("会員コードが期待値と異なります: expected=%s actual=%s", memberCode, memberCodeGot)
				}
				assertEqualInt64(t, rankNo, 1, "ランキング順位")
				assertEqualInt64(t, executionCount, 1, "約定件数")
				if totalBaseVolume != "100.000000000000000000" {
					t.Fatalf("約定数量合計が期待値と異なります: expected=100.000000000000000000 actual=%s", totalBaseVolume)
				}
				if totalNotionalVolume != "100000000.000000000000000000000000000000" {
					t.Fatalf("名目出来高合計が期待値と異なります: expected=100000000.000000000000000000000000000000 actual=%s", totalNotionalVolume)
				}
			}
		}

		if err := rows.Err(); err != nil {
			t.Fatalf("ユーザー取引ランキングの走査中に失敗しました: %v", err)
		}

		if !found {
			t.Fatal("ユーザー取引ランキングに挿入したユーザーが見つかりませんでした")
		}
	})
}

func TestUserNetFlowSummary(t *testing.T) {
	t.Run("ユーザー純流入集計_通貨別の入出金純額を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		baseDate := time.Date(2031, 2, 3, 9, 0, 0, 0, time.UTC)

		userBuilder := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID)
		memberCode := userBuilder.MemberCode
		userID := userBuilder.BuildForTest(t, ctx, tx)

		(&testdb.FiatDepositBuilder{
			UserID:          userID,
			PublicHash:      "net-flow-deposit",
			CurrencyID:      masters.JPYCurrencyID,
			Amount:          "1000",
			DepositStatusID: masters.CompletedDepositStatusID,
			RequestedAt:     baseDate,
			CompletedAt:     sql.NullTime{Time: baseDate.Add(10 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)

		(&testdb.FiatWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "net-flow-withdrawal",
			CurrencyID:         masters.JPYCurrencyID,
			Amount:             "200",
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        baseDate.Add(20 * time.Minute),
			CompletedAt:        sql.NullTime{Time: baseDate.Add(30 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/user_net_flow_summary.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				userIDGot       int64
				memberCodeGot   string
				currencyCode    string
				firstEventAt    time.Time
				lastEventAt     time.Time
				depositCount    int64
				withdrawalCount int64
				totalInAmount   string
				totalOutAmount  string
				netAmount       string
			)
			if err := rows.Scan(
				&userIDGot,
				&memberCodeGot,
				&currencyCode,
				&firstEventAt,
				&lastEventAt,
				&depositCount,
				&withdrawalCount,
				&totalInAmount,
				&totalOutAmount,
				&netAmount,
			); err != nil {
				t.Fatalf("ユーザー純流入集計の行読み取りに失敗しました: %v", err)
			}

			if userIDGot == userID && currencyCode == "JPY" {
				found = true
				if memberCodeGot != memberCode {
					t.Fatalf("会員コードが期待値と異なります: expected=%s actual=%s", memberCode, memberCodeGot)
				}
				assertEqualInt64(t, depositCount, 1, "入金件数")
				assertEqualInt64(t, withdrawalCount, 1, "出金件数")
				if totalInAmount != "1000.000000000000000000" {
					t.Fatalf("総入金額が期待値と異なります: expected=1000.000000000000000000 actual=%s", totalInAmount)
				}
				if totalOutAmount != "200.000000000000000000" {
					t.Fatalf("総出金額が期待値と異なります: expected=200.000000000000000000 actual=%s", totalOutAmount)
				}
				if netAmount != "800.000000000000000000" {
					t.Fatalf("純流入額が期待値と異なります: expected=800.000000000000000000 actual=%s", netAmount)
				}
			}
		}

		if err := rows.Err(); err != nil {
			t.Fatalf("ユーザー純流入集計の走査中に失敗しました: %v", err)
		}

		if !found {
			t.Fatal("ユーザー純流入集計に挿入したユーザーの行が見つかりませんでした")
		}
	})
}

func TestSuspiciousRapidOutflowCandidates(t *testing.T) {
	t.Run("疑わしい取引_入金直後の急速出金候補を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)

		userID := testdb.NewUserBuilder().
			WithStatusID(masters.ActiveUserStatusID).
			BuildForTest(t, ctx, tx)

		depositCompletedAt := time.Date(2031, 3, 1, 9, 5, 0, 0, time.UTC)
		depositID := (&testdb.FiatDepositBuilder{
			UserID:          userID,
			PublicHash:      "rapid-outflow-deposit-test",
			CurrencyID:      masters.JPYCurrencyID,
			Amount:          "1000000",
			DepositStatusID: masters.CompletedDepositStatusID,
			RequestedAt:     depositCompletedAt.Add(-5 * time.Minute),
			CompletedAt:     sql.NullTime{Time: depositCompletedAt, Valid: true},
		}).BuildForTest(t, ctx, tx)

		_ = (&testdb.FiatWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "rapid-outflow-withdrawal-test",
			CurrencyID:         masters.JPYCurrencyID,
			Amount:             "900000",
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        depositCompletedAt.Add(3 * time.Hour),
			CompletedAt:        sql.NullTime{Time: depositCompletedAt.Add(3*time.Hour + 20*time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/suspicious_transactions/suspicious_rapid_outflow_candidates.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				userIDGot            int64
				memberCode           string
				currencyCode         string
				inflowType           string
				inflowID             int64
				inflowCompletedAtGot time.Time
				inflowAmount         string
				matchedCount         int64
				matchedAmount        string
				outflowRatio         string
			)
			if err := rows.Scan(
				&userIDGot,
				&memberCode,
				&currencyCode,
				&inflowType,
				&inflowID,
				&inflowCompletedAtGot,
				&inflowAmount,
				&matchedCount,
				&matchedAmount,
				&outflowRatio,
			); err != nil {
				t.Fatalf("疑わしい急速出金候補の行読み取りに失敗しました: %v", err)
			}

			if userIDGot == userID {
				found = true
				if currencyCode != "JPY" {
					t.Fatalf("通貨コードが期待値と異なります: expected=JPY actual=%s", currencyCode)
				}
				if inflowType != "FIAT_DEPOSIT" {
					t.Fatalf("流入種別が期待値と異なります: expected=FIAT_DEPOSIT actual=%s", inflowType)
				}
				if inflowID != depositID {
					t.Fatalf("流入IDが期待値と異なります: expected=%d actual=%d", depositID, inflowID)
				}
				assertEqualInt64(t, matchedCount, 1, "対応する出金件数")
				if outflowRatio != "0.9000" {
					t.Fatalf("出金比率が期待値と異なります: expected=0.9000 actual=%s", outflowRatio)
				}
			}
		}

		if err := rows.Err(); err != nil {
			t.Fatalf("疑わしい急速出金候補の走査中に失敗しました: %v", err)
		}

		if !found {
			t.Fatalf("疑わしい急速出金候補に挿入したユーザーID=%d の行が見つかりませんでした", userID)
		}
	})
}

func TestSuspiciousOpenAlertCaseQueue(t *testing.T) {
	t.Run("疑わしい取引_未処理アラートとケース状況を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)

		userID := testdb.NewUserBuilder().
			WithStatusID(masters.ActiveUserStatusID).
			BuildForTest(t, ctx, tx)

		withdrawalID := (&testdb.FiatWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "case-queue-withdrawal-test",
			CurrencyID:         masters.JPYCurrencyID,
			Amount:             "750000",
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        time.Date(2031, 3, 2, 10, 0, 0, 0, time.UTC),
			CompletedAt:        sql.NullTime{Time: time.Date(2031, 3, 2, 10, 30, 0, 0, time.UTC), Valid: true},
		}).BuildForTest(t, ctx, tx)

		ruleID := (&testdb.AlertRuleBuilder{
			PublicRuleHash: "1111111111111111111111111111111111111111111111111111111111111111",
			RuleName:       "Test Rapid Outflow Rule",
			RuleType:       "RAPID_OUTFLOW",
			Severity:       "CRITICAL",
			ThresholdJSON:  `{"window_hours":24,"outflow_ratio":0.8}`,
		}).BuildForTest(t, ctx, tx)

		alertID := (&testdb.AlertEventLogBuilder{
			UserID:             userID,
			RuleID:             ruleID,
			AlertEventStatusID: masters.OpenAlertStatusID,
			FiatWithdrawalID:   withdrawalID,
			TradeExecutionID:   sql.NullInt64{},
			Score:              "98.5000",
			DetectedAt:         time.Date(2031, 3, 2, 10, 35, 0, 0, time.UTC),
			Note:               "integration-test-open-alert",
		}).BuildForTest(t, ctx, tx)

		(&testdb.SuspiciousCaseBuilder{
			UserID:          userID,
			PublicCaseHash:  "2222222222222222222222222222222222222222222222222222222222222222",
			OpenedByTypeID:  masters.SystemActorTypeID,
			OpenedByID:      "integration-test-runner",
			SourceTypeID:    masters.AutoCaseSourceTypeID,
			AlertEventLogID: sql.NullInt64{Int64: alertID, Valid: true},
			Title:           "Integration test alert case",
			CurrentStatusID: masters.InvestigatingCaseStatusID,
			RiskLevelID:     masters.CriticalRiskLevelID,
			OpenedAt:        time.Date(2031, 3, 2, 10, 40, 0, 0, time.UTC),
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/suspicious_transactions/suspicious_open_alert_case_queue.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				alertEventID       int64
				userIDGot          int64
				memberCode         string
				ruleName           string
				ruleType           string
				severity           string
				alertEventStatus   string
				score              string
				detectedAt         time.Time
				tradeExecutionID   sql.NullInt64
				fiatDepositID      sql.NullInt64
				fiatWithdrawalID   sql.NullInt64
				cryptoDepositID    sql.NullInt64
				cryptoWithdrawalID sql.NullInt64
				linkedCaseID       sql.NullInt64
				caseStatus         sql.NullString
				riskLevel          sql.NullString
				caseOpenedAt       sql.NullTime
				caseClosedAt       sql.NullTime
				note               string
			)
			if err := rows.Scan(
				&alertEventID,
				&userIDGot,
				&memberCode,
				&ruleName,
				&ruleType,
				&severity,
				&alertEventStatus,
				&score,
				&detectedAt,
				&tradeExecutionID,
				&fiatDepositID,
				&fiatWithdrawalID,
				&cryptoDepositID,
				&cryptoWithdrawalID,
				&linkedCaseID,
				&caseStatus,
				&riskLevel,
				&caseOpenedAt,
				&caseClosedAt,
				&note,
			); err != nil {
				t.Fatalf("未処理アラートキューの行読み取りに失敗しました: %v", err)
			}

			if note == "integration-test-open-alert" {
				found = true
				if userIDGot != userID {
					t.Fatalf("ユーザーIDが期待値と異なります: expected=%d actual=%d", userID, userIDGot)
				}
				if ruleName != "Test Rapid Outflow Rule" {
					t.Fatalf("ルール名が期待値と異なります: expected=Test Rapid Outflow Rule actual=%s", ruleName)
				}
				if alertEventStatus != "OPEN" {
					t.Fatalf("アラートステータスが期待値と異なります: expected=OPEN actual=%s", alertEventStatus)
				}
				if !linkedCaseID.Valid {
					t.Fatal("紐づくケースIDが返ってきませんでした")
				}
				if !caseStatus.Valid || caseStatus.String != "INVESTIGATING" {
					t.Fatalf("ケースステータスが期待値と異なります: expected=INVESTIGATING actual=%v", caseStatus)
				}
				if !riskLevel.Valid || riskLevel.String != "CRITICAL" {
					t.Fatalf("リスクレベルが期待値と異なります: expected=CRITICAL actual=%v", riskLevel)
				}
			}
		}

		if err := rows.Err(); err != nil {
			t.Fatalf("未処理アラートキューの走査中に失敗しました: %v", err)
		}

		if !found {
			t.Fatal("未処理アラートキューに挿入したアラート行が見つかりませんでした")
		}
	})
}

func TestSuspiciousThresholdEvasionTradeCandidates(t *testing.T) {
	t.Run("疑わしい取引_敷居値直下へ分散した売買候補を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		base := time.Date(2031, 3, 3, 9, 0, 0, 0, time.UTC)

		userBuilder := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID)
		memberCode := userBuilder.MemberCode
		userID := userBuilder.BuildForTest(t, ctx, tx)

		for i, amount := range []string{"990000", "995000", "998000"} {
			orderID := (&testdb.TradingOrderBuilder{
				UserID:         userID,
				PublicHash:     fmt.Sprintf("threshold-evasion-order-%d", i+1),
				Side:           "BUY",
				OrderType:      "LIMIT",
				FromCurrencyID: masters.JPYCurrencyID,
				ToCurrencyID:   masters.BTCurrencyID,
				Price:          "1000000",
				Quantity:       "1.00000000",
				OrderStatusID:  masters.FilledOrderStatusID,
				PlacedAt:       base.Add(time.Duration(i) * time.Hour),
			}).BuildForTest(t, ctx, tx)

			(&testdb.TradeExecutionBuilder{
				OrderID:          orderID,
				UserID:           userID,
				PublicHash:       fmt.Sprintf("threshold-evasion-execution-%d", i+1),
				FromCurrencyID:   masters.JPYCurrencyID,
				ToCurrencyID:     masters.BTCurrencyID,
				ExecutedPrice:    "1000000",
				ExecutedQuantity: "1.00000000",
				FromAmount:       amount,
				ToAmount:         "1.00000000",
				FeeCurrencyID:    masters.JPYCurrencyID,
				FeeAmount:        "1000",
				ExecutedAt:       base.Add(time.Duration(i) * time.Hour),
			}).BuildForTest(t, ctx, tx)
		}

		rows := queryRows(t, ctx, tx, "examples/suspicious_transactions/suspicious_threshold_evasion_trade_candidates.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				userIDGot           int64
				memberCodeGot       string
				fromCurrencyCode    string
				toCurrencyCode      string
				tradeCount          int64
				totalFromAmount     string
				avgFromAmount       string
				minFromAmount       string
				maxFromAmount       string
				firstExecutedAt     time.Time
				lastExecutedAt      time.Time
				thresholdGapPercent string
			)
			if err := rows.Scan(
				&userIDGot,
				&memberCodeGot,
				&fromCurrencyCode,
				&toCurrencyCode,
				&tradeCount,
				&totalFromAmount,
				&avgFromAmount,
				&minFromAmount,
				&maxFromAmount,
				&firstExecutedAt,
				&lastExecutedAt,
				&thresholdGapPercent,
			); err != nil {
				t.Fatalf("敷居値直下分散売買SQLの行読み取りに失敗しました: %v", err)
			}
			_ = firstExecutedAt
			_ = lastExecutedAt

			if userIDGot == userID {
				found = true
				if memberCodeGot != memberCode || fromCurrencyCode != "JPY" || toCurrencyCode != "BTC" {
					t.Fatalf("敷居値直下分散売買SQLの属性が期待値と異なります: member=%s from=%s to=%s", memberCodeGot, fromCurrencyCode, toCurrencyCode)
				}
				assertEqualInt64(t, tradeCount, 3, "敷居値直下分散売買の件数")
				if totalFromAmount != "2983000.000000000000000000" {
					t.Fatalf("敷居値直下分散売買の総額が期待値と異なります: actual=%s", totalFromAmount)
				}
				if minFromAmount != "990000.000000000000000000" || maxFromAmount != "998000.000000000000000000" {
					t.Fatalf("敷居値直下分散売買の最小最大が期待値と異なります: min=%s max=%s", minFromAmount, maxFromAmount)
				}
				if thresholdGapPercent != "0.2000" {
					t.Fatalf("敷居値直下分散売買の閾値乖離率が期待値と異なります: actual=%s", thresholdGapPercent)
				}
				if avgFromAmount != "994333.333333333333333333" {
					t.Fatalf("敷居値直下分散売買の平均額が期待値と異なります: actual=%s", avgFromAmount)
				}
			}
		}

		if err := rows.Err(); err != nil {
			t.Fatalf("敷居値直下分散売買SQLの走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("敷居値直下分散売買SQLに挿入した行が見つかりませんでした")
		}
	})
}

func TestSuspiciousSmallThenLargeTradeBurst(t *testing.T) {
	t.Run("疑わしい取引_少額成功直後の高額連続売買を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		base := time.Date(2031, 3, 4, 10, 0, 0, 0, time.UTC)

		userBuilder := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID)
		memberCode := userBuilder.MemberCode
		userID := userBuilder.BuildForTest(t, ctx, tx)

		smallOrderID := (&testdb.TradingOrderBuilder{
			UserID:         userID,
			PublicHash:     "small-then-large-order-1",
			Side:           "BUY",
			OrderType:      "LIMIT",
			FromCurrencyID: masters.JPYCurrencyID,
			ToCurrencyID:   masters.BTCurrencyID,
			Price:          "1000000",
			Quantity:       "0.00100000",
			OrderStatusID:  masters.FilledOrderStatusID,
			PlacedAt:       base,
		}).BuildForTest(t, ctx, tx)
		(&testdb.TradeExecutionBuilder{
			OrderID:          smallOrderID,
			UserID:           userID,
			PublicHash:       "small-then-large-execution-1",
			FromCurrencyID:   masters.JPYCurrencyID,
			ToCurrencyID:     masters.BTCurrencyID,
			ExecutedPrice:    "1000000",
			ExecutedQuantity: "0.00100000",
			FromAmount:       "1000",
			ToAmount:         "0.00100000",
			FeeCurrencyID:    masters.JPYCurrencyID,
			FeeAmount:        "10",
			ExecutedAt:       base.Add(5 * time.Minute),
		}).BuildForTest(t, ctx, tx)

		for i, amount := range []string{"1500000", "1800000"} {
			orderID := (&testdb.TradingOrderBuilder{
				UserID:         userID,
				PublicHash:     fmt.Sprintf("small-then-large-order-%d", i+2),
				Side:           "BUY",
				OrderType:      "LIMIT",
				FromCurrencyID: masters.JPYCurrencyID,
				ToCurrencyID:   masters.BTCurrencyID,
				Price:          "1000000",
				Quantity:       "1.50000000",
				OrderStatusID:  masters.FilledOrderStatusID,
				PlacedAt:       base.Add(time.Duration(i+1) * time.Hour),
			}).BuildForTest(t, ctx, tx)
			(&testdb.TradeExecutionBuilder{
				OrderID:          orderID,
				UserID:           userID,
				PublicHash:       fmt.Sprintf("small-then-large-execution-%d", i+2),
				FromCurrencyID:   masters.JPYCurrencyID,
				ToCurrencyID:     masters.BTCurrencyID,
				ExecutedPrice:    "1000000",
				ExecutedQuantity: "1.50000000",
				FromAmount:       amount,
				ToAmount:         "1.50000000",
				FeeCurrencyID:    masters.JPYCurrencyID,
				FeeAmount:        "1000",
				ExecutedAt:       base.Add(time.Duration(i+1) * time.Hour),
			}).BuildForTest(t, ctx, tx)
		}

		rows := queryRows(t, ctx, tx, "examples/suspicious_transactions/suspicious_small_then_large_trade_burst.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				userIDGot            int64
				memberCodeGot        string
				fromCurrencyCode     string
				toCurrencyCode       string
				initialExecutedAt    time.Time
				initialFromAmount    string
				largeTradeCount      int64
				largeTradeTotal      string
				firstLargeExecutedAt time.Time
				lastLargeExecutedAt  time.Time
				burstHours           string
			)
			if err := rows.Scan(
				&userIDGot,
				&memberCodeGot,
				&fromCurrencyCode,
				&toCurrencyCode,
				&initialExecutedAt,
				&initialFromAmount,
				&largeTradeCount,
				&largeTradeTotal,
				&firstLargeExecutedAt,
				&lastLargeExecutedAt,
				&burstHours,
			); err != nil {
				t.Fatalf("少額成功直後の高額連続売買SQLの行読み取りに失敗しました: %v", err)
			}
			_ = initialExecutedAt
			_ = firstLargeExecutedAt
			_ = lastLargeExecutedAt

			if userIDGot == userID {
				found = true
				if memberCodeGot != memberCode || fromCurrencyCode != "JPY" || toCurrencyCode != "BTC" {
					t.Fatalf("少額成功直後の高額連続売買SQLの属性が期待値と異なります: member=%s from=%s to=%s", memberCodeGot, fromCurrencyCode, toCurrencyCode)
				}
				if initialFromAmount != "1000.000000000000000000" {
					t.Fatalf("少額成功直後の高額連続売買SQLの初回少額が期待値と異なります: actual=%s", initialFromAmount)
				}
				assertEqualInt64(t, largeTradeCount, 2, "高額連続売買の件数")
				if largeTradeTotal != "3300000.000000000000000000" {
					t.Fatalf("高額連続売買の総額が期待値と異なります: actual=%s", largeTradeTotal)
				}
				if burstHours != "1.92" {
					t.Fatalf("高額連続売買の経過時間が期待値と異なります: actual=%s", burstHours)
				}
			}
		}

		if err := rows.Err(); err != nil {
			t.Fatalf("少額成功直後の高額連続売買SQLの走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("少額成功直後の高額連続売買SQLに挿入した行が見つかりませんでした")
		}
	})
}

func TestAlertRuleDetectionSummary(t *testing.T) {
	t.Run("ルール別検知集計_検知件数とケース化率を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)

		userID := testdb.NewUserBuilder().
			WithStatusID(masters.ActiveUserStatusID).
			BuildForTest(t, ctx, tx)

		withdrawalID := (&testdb.FiatWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "alert-summary-withdrawal",
			CurrencyID:         masters.JPYCurrencyID,
			Amount:             "880000",
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        time.Date(2031, 3, 3, 9, 0, 0, 0, time.UTC),
			CompletedAt:        sql.NullTime{Time: time.Date(2031, 3, 3, 9, 20, 0, 0, time.UTC), Valid: true},
		}).BuildForTest(t, ctx, tx)

		ruleID := (&testdb.AlertRuleBuilder{
			PublicRuleHash: "3333333333333333333333333333333333333333333333333333333333333333",
			RuleName:       "Integration Rule Detection Summary",
			RuleType:       "LARGE_WITHDRAWAL",
			Severity:       "HIGH",
			ThresholdJSON:  `{"single_withdrawal_amount":800000}`,
		}).BuildForTest(t, ctx, tx)

		alertID := (&testdb.AlertEventLogBuilder{
			UserID:             userID,
			RuleID:             ruleID,
			AlertEventStatusID: masters.ReviewedAlertStatusID,
			FiatWithdrawalID:   withdrawalID,
			TradeExecutionID:   sql.NullInt64{},
			Score:              "77.7000",
			DetectedAt:         time.Date(2031, 3, 3, 9, 25, 0, 0, time.UTC),
			Note:               "integration-rule-detection-summary",
		}).BuildForTest(t, ctx, tx)

		(&testdb.SuspiciousCaseBuilder{
			UserID:          userID,
			PublicCaseHash:  "4444444444444444444444444444444444444444444444444444444444444444",
			OpenedByTypeID:  masters.SystemActorTypeID,
			OpenedByID:      "integration-test-runner",
			SourceTypeID:    masters.AutoCaseSourceTypeID,
			AlertEventLogID: sql.NullInt64{Int64: alertID, Valid: true},
			Title:           "Integration rule summary case",
			CurrentStatusID: masters.OpenCaseStatusID,
			RiskLevelID:     masters.CriticalRiskLevelID,
			OpenedAt:        time.Date(2031, 3, 3, 9, 30, 0, 0, time.UTC),
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/suspicious_transactions/alert_rule_detection_summary.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				ruleIDGot        int64
				ruleName         string
				ruleType         string
				severity         string
				detectionCount   int64
				activeAlertCount int64
				linkedCaseCount  int64
				caseLinkRatePct  string
				avgScore         string
				firstDetectedAt  time.Time
				lastDetectedAt   time.Time
			)
			if err := rows.Scan(
				&ruleIDGot,
				&ruleName,
				&ruleType,
				&severity,
				&detectionCount,
				&activeAlertCount,
				&linkedCaseCount,
				&caseLinkRatePct,
				&avgScore,
				&firstDetectedAt,
				&lastDetectedAt,
			); err != nil {
				t.Fatalf("ルール別検知集計の行読み取りに失敗しました: %v", err)
			}

			if ruleIDGot == ruleID {
				found = true
				if ruleName != "Integration Rule Detection Summary" {
					t.Fatalf("ルール名が期待値と異なります: expected=Integration Rule Detection Summary actual=%s", ruleName)
				}
				if ruleType != "LARGE_WITHDRAWAL" {
					t.Fatalf("ルール種別が期待値と異なります: expected=LARGE_WITHDRAWAL actual=%s", ruleType)
				}
				if severity != "HIGH" {
					t.Fatalf("重要度が期待値と異なります: expected=HIGH actual=%s", severity)
				}
				assertEqualInt64(t, detectionCount, 1, "検知件数")
				assertEqualInt64(t, activeAlertCount, 1, "有効アラート件数")
				assertEqualInt64(t, linkedCaseCount, 1, "ケース紐づき件数")
				if caseLinkRatePct != "100.00" {
					t.Fatalf("ケース化率が期待値と異なります: expected=100.00 actual=%s", caseLinkRatePct)
				}
				if avgScore != "77.7000" {
					t.Fatalf("平均スコアが期待値と異なります: expected=77.7000 actual=%s", avgScore)
				}
			}
		}

		if err := rows.Err(); err != nil {
			t.Fatalf("ルール別検知集計の走査中に失敗しました: %v", err)
		}

		if !found {
			t.Fatalf("ルール別検知集計に挿入したルールID=%d の行が見つかりませんでした", ruleID)
		}
	})
}

func TestCaseBacklogSummary(t *testing.T) {
	t.Run("ケース滞留集計_ステータス別リスク別の滞留件数を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)

		userID := testdb.NewUserBuilder().
			WithStatusID(masters.ActiveUserStatusID).
			BuildForTest(t, ctx, tx)

		openedAt := time.Date(2031, 3, 4, 10, 0, 0, 0, time.UTC)
		(&testdb.SuspiciousCaseBuilder{
			UserID:          userID,
			PublicCaseHash:  "5555555555555555555555555555555555555555555555555555555555555555",
			OpenedByTypeID:  masters.SystemActorTypeID,
			OpenedByID:      "integration-test-runner",
			SourceTypeID:    masters.AutoCaseSourceTypeID,
			AlertEventLogID: sql.NullInt64{},
			Title:           "Integration backlog summary case",
			CurrentStatusID: masters.OpenCaseStatusID,
			RiskLevelID:     masters.CriticalRiskLevelID,
			OpenedAt:        openedAt,
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/suspicious_transactions/case_backlog_summary.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				caseStatus     string
				riskLevel      string
				backlogCount   int64
				avgOpenDays    string
				oldestOpenDays int64
				oldestOpenedAt time.Time
				newestOpenedAt time.Time
			)
			if err := rows.Scan(
				&caseStatus,
				&riskLevel,
				&backlogCount,
				&avgOpenDays,
				&oldestOpenDays,
				&oldestOpenedAt,
				&newestOpenedAt,
			); err != nil {
				t.Fatalf("ケース滞留集計の行読み取りに失敗しました: %v", err)
			}

			if caseStatus == "OPEN" && riskLevel == "CRITICAL" {
				found = true
				assertEqualInt64(t, backlogCount, 1, "滞留件数")
				if !oldestOpenedAt.Equal(openedAt) {
					t.Fatalf("最古起票日時が期待値と異なります: expected=%s actual=%s", openedAt.Format(time.RFC3339), oldestOpenedAt.Format(time.RFC3339))
				}
				if !newestOpenedAt.Equal(openedAt) {
					t.Fatalf("最新起票日時が期待値と異なります: expected=%s actual=%s", openedAt.Format(time.RFC3339), newestOpenedAt.Format(time.RFC3339))
				}
			}
		}

		if err := rows.Err(); err != nil {
			t.Fatalf("ケース滞留集計の走査中に失敗しました: %v", err)
		}

		if !found {
			t.Fatal("ケース滞留集計に挿入した OPEN / CRITICAL の行が見つかりませんでした")
		}
	})
}

func TestSuspiciousWithdrawalConcentrationCandidates(t *testing.T) {
	t.Run("疑わしい出金集中候補_直近24時間の出金集中ユーザーを返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		now := currentDBTime(t, ctx, tx)

		userBuilder := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID)
		memberCode := userBuilder.MemberCode
		userID := userBuilder.BuildForTest(t, ctx, tx)

		(&testdb.FiatDepositBuilder{
			UserID:          userID,
			PublicHash:      "withdrawal-concentration-deposit",
			CurrencyID:      masters.JPYCurrencyID,
			Amount:          "100",
			DepositStatusID: masters.CompletedDepositStatusID,
			RequestedAt:     now.Add(-20 * time.Hour),
			CompletedAt:     sql.NullTime{Time: now.Add(-19*time.Hour - 50*time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)

		(&testdb.FiatWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "withdrawal-concentration-w1",
			CurrencyID:         masters.JPYCurrencyID,
			Amount:             "200",
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        now.Add(-18 * time.Hour),
			CompletedAt:        sql.NullTime{Time: now.Add(-17*time.Hour - 30*time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)
		(&testdb.FiatWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "withdrawal-concentration-w2",
			CurrencyID:         masters.JPYCurrencyID,
			Amount:             "250",
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        now.Add(-10 * time.Hour),
			CompletedAt:        sql.NullTime{Time: now.Add(-9*time.Hour - 30*time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)
		(&testdb.FiatWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "withdrawal-concentration-w3",
			CurrencyID:         masters.JPYCurrencyID,
			Amount:             "300",
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        now.Add(-3 * time.Hour),
			CompletedAt:        sql.NullTime{Time: now.Add(-2*time.Hour - 30*time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/suspicious_transactions/suspicious_withdrawal_concentration_candidates.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				userIDGot         int64
				memberCodeGot     string
				currencyCode      string
				inflowCount24h    int64
				outflowCount24h   int64
				totalInAmount24h  string
				totalOutAmount24h string
				netOutAmount24h   string
				lastEventAt       time.Time
			)
			if err := rows.Scan(
				&userIDGot,
				&memberCodeGot,
				&currencyCode,
				&inflowCount24h,
				&outflowCount24h,
				&totalInAmount24h,
				&totalOutAmount24h,
				&netOutAmount24h,
				&lastEventAt,
			); err != nil {
				t.Fatalf("疑わしい出金集中候補の行読み取りに失敗しました: %v", err)
			}

			if userIDGot == userID {
				found = true
				if memberCodeGot != memberCode {
					t.Fatalf("会員コードが期待値と異なります: expected=%s actual=%s", memberCode, memberCodeGot)
				}
				if currencyCode != "JPY" {
					t.Fatalf("通貨コードが期待値と異なります: expected=JPY actual=%s", currencyCode)
				}
				assertEqualInt64(t, inflowCount24h, 1, "24時間入金件数")
				assertEqualInt64(t, outflowCount24h, 3, "24時間出金件数")
				if totalInAmount24h != "100.000000000000000000" {
					t.Fatalf("24時間入金総額が期待値と異なります: expected=100.000000000000000000 actual=%s", totalInAmount24h)
				}
				if totalOutAmount24h != "750.000000000000000000" {
					t.Fatalf("24時間出金総額が期待値と異なります: expected=750.000000000000000000 actual=%s", totalOutAmount24h)
				}
				if netOutAmount24h != "650.000000000000000000" {
					t.Fatalf("24時間純流出超過額が期待値と異なります: expected=650.000000000000000000 actual=%s", netOutAmount24h)
				}
			}
		}

		if err := rows.Err(); err != nil {
			t.Fatalf("疑わしい出金集中候補の走査中に失敗しました: %v", err)
		}

		if !found {
			t.Fatal("疑わしい出金集中候補に挿入したユーザーの行が見つかりませんでした")
		}
	})
}

func TestUserStatusTimeline(t *testing.T) {
	t.Run("ユーザーステータスタイムライン_状態遷移を理由付きで返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)

		userBuilder := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID)
		memberCode := userBuilder.MemberCode
		userID := userBuilder.BuildForTest(t, ctx, tx)
		occurredAt := time.Date(2031, 3, 5, 9, 0, 0, 0, time.UTC)

		eventID := (&testdb.UserStatusChangeEventBuilder{
			UserID:      userID,
			EventTypeID: masters.FrozenEventTypeID,
			ActorTypeID: masters.AdminActorTypeID,
			ActorID:     "admin-001",
			Reason:      "suspicious transfer detected",
			OccurredAt:  occurredAt,
		}).BuildForTest(t, ctx, tx)

		(&testdb.UserStatusHistoryBuilder{
			UserID:              userID,
			StatusChangeEventID: eventID,
			FromStatusID:        sql.NullInt64{Int64: masters.ActiveUserStatusID, Valid: true},
			ToStatusID:          masters.FrozenUserStatusID,
			ChangedAt:           occurredAt.Add(5 * time.Minute),
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/user_status_timeline.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				userIDGot     int64
				memberCodeGot string
				fromStatus    sql.NullString
				toStatus      string
				eventType     string
				actorType     string
				actorID       string
				reason        string
				gotOccurredAt time.Time
				changedAt     time.Time
			)
			if err := rows.Scan(&userIDGot, &memberCodeGot, &fromStatus, &toStatus, &eventType, &actorType, &actorID, &reason, &gotOccurredAt, &changedAt); err != nil {
				t.Fatalf("ユーザーステータスタイムラインの行読み取りに失敗しました: %v", err)
			}

			if userIDGot == userID {
				found = true
				if memberCodeGot != memberCode {
					t.Fatalf("会員コードが期待値と異なります: expected=%s actual=%s", memberCode, memberCodeGot)
				}
				if !fromStatus.Valid || fromStatus.String != "ACTIVE" {
					t.Fatalf("変更前ステータスが期待値と異なります: expected=ACTIVE actual=%v", fromStatus)
				}
				if toStatus != "FROZEN" {
					t.Fatalf("変更後ステータスが期待値と異なります: expected=FROZEN actual=%s", toStatus)
				}
				if eventType != "FROZEN" || actorType != "ADMIN" || actorID != "admin-001" {
					t.Fatalf("イベント情報が期待値と異なります: eventType=%s actorType=%s actorID=%s", eventType, actorType, actorID)
				}
				if reason != "suspicious transfer detected" {
					t.Fatalf("理由が期待値と異なります: expected=suspicious transfer detected actual=%s", reason)
				}
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("ユーザーステータスタイムラインの走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("ユーザーステータスタイムラインに挿入した状態遷移が見つかりませんでした")
		}
	})
}

func TestOpenOrdersStalenessSummary(t *testing.T) {
	t.Run("OPEN注文滞留集計_滞留バケットごとの件数を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)

		userID := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID).BuildForTest(t, ctx, tx)
		dbNow := currentDBTime(t, ctx, tx)
		placedAt := dbNow.Add(-30 * time.Minute)

		(&testdb.TradingOrderBuilder{
			UserID:         userID,
			PublicHash:     "open-order-staleness-test",
			Side:           "SELL",
			OrderType:      "LIMIT",
			FromCurrencyID: masters.BTCurrencyID,
			ToCurrencyID:   masters.JPYCurrencyID,
			Price:          "6000000",
			Quantity:       "0.50000000",
			OrderStatusID:  masters.OpenOrderStatusID,
			PlacedAt:       placedAt,
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/open_orders_staleness_summary.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				bucket         string
				openOrderCount int64
				oldestOrderAt  time.Time
				newestOrderAt  time.Time
				avgPrice       string
				avgQuantity    string
			)
			if err := rows.Scan(&bucket, &openOrderCount, &oldestOrderAt, &newestOrderAt, &avgPrice, &avgQuantity); err != nil {
				t.Fatalf("OPEN注文滞留集計の行読み取りに失敗しました: %v", err)
			}

			if bucket == "UNDER_1_HOUR" {
				found = true
				assertGreaterOrEqualInt64(t, openOrderCount, 1, "OPEN注文件数")
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("OPEN注文滞留集計の走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("OPEN注文滞留集計に UNDER_1_HOUR バケットが見つかりませんでした")
		}
	})
}

func TestLargeFailedTransactions(t *testing.T) {
	t.Run("高額失敗取引一覧_高額失敗取引を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)

		userBuilder := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID)
		memberCode := userBuilder.MemberCode
		userID := userBuilder.BuildForTest(t, ctx, tx)
		failedAt := time.Date(2031, 3, 6, 11, 0, 0, 0, time.UTC)

		(&testdb.FiatWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "large-failed-withdrawal-test",
			CurrencyID:         masters.JPYCurrencyID,
			Amount:             "9999999",
			WithdrawalStatusID: masters.FailedWithdrawalID,
			RequestedAt:        failedAt.Add(-20 * time.Minute),
			FailedAt:           sql.NullTime{Time: failedAt, Valid: true},
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/large_failed_transactions.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				operationType string
				userIDGot     int64
				memberCodeGot string
				currencyCode  string
				amount        string
				requestedAt   time.Time
				gotFailedAt   time.Time
				publicID      string
			)
			if err := rows.Scan(&operationType, &userIDGot, &memberCodeGot, &currencyCode, &amount, &requestedAt, &gotFailedAt, &publicID); err != nil {
				t.Fatalf("高額失敗取引一覧の行読み取りに失敗しました: %v", err)
			}

			if publicID == "large-failed-withdrawal-test" {
				// unreachable because SQL returns hash, so keep scanning by user and amount below
			}
			if userIDGot == userID && amount == "9999999.000000000000000000" {
				found = true
				if operationType != "FIAT_WITHDRAWAL" || memberCodeGot != memberCode || currencyCode != "JPY" {
					t.Fatalf("高額失敗取引の属性が期待値と異なります: operationType=%s memberCode=%s currency=%s", operationType, memberCodeGot, currencyCode)
				}
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("高額失敗取引一覧の走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("高額失敗取引一覧に挿入した失敗取引が見つかりませんでした")
		}
	})
}

func TestAccountActionSummary(t *testing.T) {
	t.Run("口座措置集計_措置種別と実行者種別ごとの件数を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)

		userID := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID).BuildForTest(t, ctx, tx)
		requestedAt := time.Date(2031, 3, 7, 8, 0, 0, 0, time.UTC)

		(&testdb.AccountActionBuilder{
			UserID:           userID,
			SuspiciousCaseID: sql.NullInt64{},
			ActionTypeID:     masters.FreezeActionTypeID,
			ActorTypeID:      masters.AdminActorTypeID,
			ActorID:          "admin-002",
			ActionReason:     "manual freeze",
			RequestedAt:      requestedAt,
			CompletedAt:      sql.NullTime{Time: requestedAt.Add(15 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/account_action_summary.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				actionDate     time.Time
				actionType     string
				actorType      string
				actionCount    int64
				completedCount int64
				firstRequested time.Time
				lastRequested  time.Time
			)
			if err := rows.Scan(&actionDate, &actionType, &actorType, &actionCount, &completedCount, &firstRequested, &lastRequested); err != nil {
				t.Fatalf("口座措置集計の行読み取りに失敗しました: %v", err)
			}

			if actionDate.Format("2006-01-02") == "2031-03-07" && actionType == "FREEZE" && actorType == "ADMIN" {
				found = true
				assertEqualInt64(t, actionCount, 1, "口座措置件数")
				assertEqualInt64(t, completedCount, 1, "完了件数")
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("口座措置集計の走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("口座措置集計に挿入した FREEZE / ADMIN の行が見つかりませんでした")
		}
	})
}

func TestCaseLeadTimeSummary(t *testing.T) {
	t.Run("ケースリードタイム集計_クローズ済みケースの所要日数を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)

		userID := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID).BuildForTest(t, ctx, tx)
		openedAt := time.Date(2031, 3, 8, 9, 0, 0, 0, time.UTC)
		closedAt := openedAt.Add(48 * time.Hour)

		(&testdb.SuspiciousCaseBuilder{
			UserID:          userID,
			PublicCaseHash:  "6666666666666666666666666666666666666666666666666666666666666666",
			OpenedByTypeID:  masters.SystemActorTypeID,
			OpenedByID:      "integration-test-runner",
			SourceTypeID:    masters.AutoCaseSourceTypeID,
			AlertEventLogID: sql.NullInt64{},
			Title:           "Integration closed case",
			CurrentStatusID: masters.ClosedCaseStatusID,
			RiskLevelID:     masters.CriticalRiskLevelID,
			OpenedAt:        openedAt,
			ClosedAt:        sql.NullTime{Time: closedAt, Valid: true},
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/suspicious_transactions/case_lead_time_summary.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				riskLevel       string
				closedCaseCount int64
				avgLeadDays     string
				maxLeadDays     string
				minLeadDays     string
			)
			if err := rows.Scan(&riskLevel, &closedCaseCount, &avgLeadDays, &maxLeadDays, &minLeadDays); err != nil {
				t.Fatalf("ケースリードタイム集計の行読み取りに失敗しました: %v", err)
			}

			if riskLevel == "CRITICAL" {
				found = true
				assertEqualInt64(t, closedCaseCount, 1, "クローズ済みケース件数")
				if avgLeadDays != "2.00" || maxLeadDays != "2.00" || minLeadDays != "2.00" {
					t.Fatalf("リードタイム日数が期待値と異なります: avg=%s max=%s min=%s", avgLeadDays, maxLeadDays, minLeadDays)
				}
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("ケースリードタイム集計の走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("ケースリードタイム集計に挿入した CRITICAL リスクのクローズ済みケースが見つかりませんでした")
		}
	})
}

func TestRuleFalsePositiveProxy(t *testing.T) {
	t.Run("誤検知proxy集計_未ケース化率と無視率を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)

		userID := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID).BuildForTest(t, ctx, tx)
		withdrawalID := (&testdb.FiatWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "false-positive-proxy-withdrawal",
			CurrencyID:         masters.JPYCurrencyID,
			Amount:             "123456",
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        time.Date(2031, 3, 9, 9, 0, 0, 0, time.UTC),
			CompletedAt:        sql.NullTime{Time: time.Date(2031, 3, 9, 9, 10, 0, 0, time.UTC), Valid: true},
		}).BuildForTest(t, ctx, tx)

		ruleID := (&testdb.AlertRuleBuilder{
			PublicRuleHash: "7777777777777777777777777777777777777777777777777777777777777777",
			RuleName:       "Integration False Positive Proxy Rule",
			RuleType:       "TEST_PROXY",
			Severity:       "MEDIUM",
			ThresholdJSON:  `{"sample":true}`,
		}).BuildForTest(t, ctx, tx)

		(&testdb.AlertEventLogBuilder{
			UserID:             userID,
			RuleID:             ruleID,
			AlertEventStatusID: masters.ReviewedAlertStatusID,
			FiatWithdrawalID:   withdrawalID,
			TradeExecutionID:   sql.NullInt64{},
			Score:              "50.0000",
			DetectedAt:         time.Date(2031, 3, 9, 9, 15, 0, 0, time.UTC),
			Note:               "integration-false-positive-proxy",
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/suspicious_transactions/rule_false_positive_proxy.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				ruleIDGot       int64
				ruleName        string
				ruleType        string
				severity        string
				detectionCount  int64
				ignoredCount    int64
				unlinkedCount   int64
				ignoredRatePct  string
				unlinkedRatePct string
			)
			if err := rows.Scan(&ruleIDGot, &ruleName, &ruleType, &severity, &detectionCount, &ignoredCount, &unlinkedCount, &ignoredRatePct, &unlinkedRatePct); err != nil {
				t.Fatalf("誤検知proxy集計の行読み取りに失敗しました: %v", err)
			}

			if ruleIDGot == ruleID {
				found = true
				if ruleName != "Integration False Positive Proxy Rule" || ruleType != "TEST_PROXY" || severity != "MEDIUM" {
					t.Fatalf("ルール属性が期待値と異なります: ruleName=%s ruleType=%s severity=%s", ruleName, ruleType, severity)
				}
				assertEqualInt64(t, detectionCount, 1, "検知件数")
				assertEqualInt64(t, ignoredCount, 0, "無視件数")
				assertEqualInt64(t, unlinkedCount, 1, "未ケース化件数")
				if ignoredRatePct != "0.00" || unlinkedRatePct != "100.00" {
					t.Fatalf("誤検知proxy率が期待値と異なります: ignoredRate=%s unlinkedRate=%s", ignoredRatePct, unlinkedRatePct)
				}
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("誤検知proxy集計の走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("誤検知proxy集計に挿入したルールの行が見つかりませんでした")
		}
	})
}

func TestUserProfileChangeTimeline(t *testing.T) {
	t.Run("プロフィール変更タイムライン_版順の履歴を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)

		userBuilder := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID)
		memberCode := userBuilder.MemberCode
		userID := userBuilder.BuildForTest(t, ctx, tx)

		(&testdb.UserProfileVersionBuilder{
			UserID:                  userID,
			VersionNo:               1,
			LastName:                "Yamada",
			FirstName:               "Taro",
			BirthDate:               time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC),
			CountryCode:             "JP",
			OccupationID:            masters.OccupationOtherID,
			AnnualIncomeBracketID:   masters.IncomeMidID,
			FinancialAssetBracketID: masters.AssetMidID,
			DeclaredAt:              time.Date(2031, 3, 10, 9, 0, 0, 0, time.UTC),
			ChangeReason:            "initial profile",
		}).BuildForTest(t, ctx, tx)
		(&testdb.UserProfileVersionBuilder{
			UserID:                  userID,
			VersionNo:               2,
			LastName:                "Yamada",
			FirstName:               "Taro",
			BirthDate:               time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC),
			CountryCode:             "JP",
			OccupationID:            masters.OccupationOtherID,
			AnnualIncomeBracketID:   masters.IncomeMidID,
			FinancialAssetBracketID: masters.AssetMidID,
			DeclaredAt:              time.Date(2031, 3, 11, 9, 0, 0, 0, time.UTC),
			ChangeReason:            "income updated",
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/user_profile_change_timeline.sql")
		defer rows.Close()

		// user_profile_versions を version_no=1,2 で投入し、
		// ORDER BY upv.declared_at DESC, u.id, upv.version_no DESC に従って
		// 新しい申告(version_no=2)が先に返ることを前提に値を確認する。
		var found bool
		for rows.Next() {
			var (
				userIDGot     int64
				memberCodeGot string
				versionNo     int64
				lastName      string
				firstName     string
				countryCode   string
				occupation    string
				incomeBracket string
				assetBracket  string
				changeReason  string
				declaredAt    time.Time
			)
			if err := rows.Scan(&userIDGot, &memberCodeGot, &versionNo, &lastName, &firstName, &countryCode, &occupation, &incomeBracket, &assetBracket, &changeReason, &declaredAt); err != nil {
				t.Fatalf("プロフィール変更タイムラインの行読み取りに失敗しました: %v", err)
			}
			if userIDGot == userID && versionNo == 2 {
				found = true
				if memberCodeGot != memberCode || changeReason != "income updated" || countryCode != "JP" {
					t.Fatalf("プロフィール変更タイムラインの値が期待値と異なります: memberCode=%s reason=%s country=%s", memberCodeGot, changeReason, countryCode)
				}
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("プロフィール変更タイムラインの走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("プロフィール変更タイムラインに挿入した version 2 の履歴が見つかりませんでした")
		}
	})
}

func TestDepositWithdrawalLeadTimeSummary(t *testing.T) {
	t.Run("入出金リードタイム集計_業務種別別の所要時間を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		userID := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID).BuildForTest(t, ctx, tx)
		base := time.Date(2031, 3, 12, 9, 0, 0, 0, time.UTC)

		(&testdb.FiatDepositBuilder{
			UserID:          userID,
			PublicHash:      "lead-time-fiat-deposit",
			CurrencyID:      masters.JPYCurrencyID,
			Amount:          "1000",
			DepositStatusID: masters.CompletedDepositStatusID,
			RequestedAt:     base,
			CompletedAt:     sql.NullTime{Time: base.Add(15 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/deposit_withdrawal_lead_time_summary.sql")
		defer rows.Close()

		// このテストでは FIAT_DEPOSIT だけを1件投入している。
		// SQL側の
		//   TIMESTAMPDIFF(MINUTE, fd.requested_at, fd.completed_at)
		// が 15 を返し、
		//   GROUP BY operation_type, activity_date
		// 後も completed_count=1 のまま残ることを確認する。
		var found bool
		for rows.Next() {
			var (
				operationType  string
				activityDate   time.Time
				completedCount int64
				avgLeadMinutes string
				maxLeadMinutes int64
				minLeadMinutes int64
			)
			if err := rows.Scan(&operationType, &activityDate, &completedCount, &avgLeadMinutes, &maxLeadMinutes, &minLeadMinutes); err != nil {
				t.Fatalf("入出金リードタイム集計の行読み取りに失敗しました: %v", err)
			}
			if operationType == "FIAT_DEPOSIT" && activityDate.Format("2006-01-02") == "2031-03-12" {
				found = true
				assertEqualInt64(t, completedCount, 1, "完了件数")
				if avgLeadMinutes != "15.00" || maxLeadMinutes != 15 || minLeadMinutes != 15 {
					t.Fatalf("リードタイムが期待値と異なります: avg=%s max=%d min=%d", avgLeadMinutes, maxLeadMinutes, minLeadMinutes)
				}
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("入出金リードタイム集計の走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("入出金リードタイム集計に挿入した法定入金の行が見つかりませんでした")
		}
	})
}

func TestCurrencyFlowDailySummary(t *testing.T) {
	t.Run("通貨別日次資金フロー_純流入を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		userID := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID).BuildForTest(t, ctx, tx)
		base := time.Date(2031, 3, 13, 9, 0, 0, 0, time.UTC)

		(&testdb.FiatDepositBuilder{
			UserID:          userID,
			PublicHash:      "currency-flow-deposit",
			CurrencyID:      masters.JPYCurrencyID,
			Amount:          "1000",
			DepositStatusID: masters.CompletedDepositStatusID,
			RequestedAt:     base,
			CompletedAt:     sql.NullTime{Time: base.Add(10 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)
		(&testdb.FiatWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "currency-flow-withdrawal",
			CurrencyID:         masters.JPYCurrencyID,
			Amount:             "400",
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        base.Add(20 * time.Minute),
			CompletedAt:        sql.NullTime{Time: base.Add(30 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/currency_flow_daily_summary.sql")
		defer rows.Close()

		// SQLでは
		//   SUM(CASE WHEN nf.direction = 'IN' THEN nf.amount ELSE 0 END)
		//   SUM(CASE WHEN nf.direction = 'OUT' THEN nf.amount ELSE 0 END)
		//   SUM(CASE WHEN nf.direction = 'IN' THEN nf.amount ELSE -nf.amount END)
		// を返している。
		// このテストでは JPY の入金 1000 と出金 400 を同日に投入し、
		// total_in_amount=1000 / total_out_amount=400 / net_amount=600 を確認する。
		var found bool
		for rows.Next() {
			var (
				activityDate   time.Time
				currencyCode   string
				totalInAmount  string
				totalOutAmount string
				netAmount      string
			)
			if err := rows.Scan(&activityDate, &currencyCode, &totalInAmount, &totalOutAmount, &netAmount); err != nil {
				t.Fatalf("通貨別日次資金フローの行読み取りに失敗しました: %v", err)
			}
			if activityDate.Format("2006-01-02") == "2031-03-13" && currencyCode == "JPY" {
				found = true
				if totalInAmount != "1000.000000000000000000" || totalOutAmount != "400.000000000000000000" || netAmount != "600.000000000000000000" {
					t.Fatalf("通貨別日次資金フローの値が期待値と異なります: in=%s out=%s net=%s", totalInAmount, totalOutAmount, netAmount)
				}
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("通貨別日次資金フローの走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("通貨別日次資金フローに挿入した JPY 行が見つかりませんでした")
		}
	})
}

func TestUserCaseOverview(t *testing.T) {
	t.Run("ユーザーケース概要_検知とケースと措置を1行で返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		userBuilder := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID)
		memberCode := userBuilder.MemberCode
		userID := userBuilder.BuildForTest(t, ctx, tx)

		withdrawalID := (&testdb.FiatWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "user-case-overview-withdrawal",
			CurrencyID:         masters.JPYCurrencyID,
			Amount:             "888888",
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        time.Date(2031, 3, 14, 9, 0, 0, 0, time.UTC),
			CompletedAt:        sql.NullTime{Time: time.Date(2031, 3, 14, 9, 20, 0, 0, time.UTC), Valid: true},
		}).BuildForTest(t, ctx, tx)
		ruleID := (&testdb.AlertRuleBuilder{
			PublicRuleHash: "8888888888888888888888888888888888888888888888888888888888888888",
			RuleName:       "User Case Overview Rule",
			RuleType:       "OVERVIEW",
			Severity:       "HIGH",
			ThresholdJSON:  `{"sample":true}`,
		}).BuildForTest(t, ctx, tx)
		alertID := (&testdb.AlertEventLogBuilder{
			UserID:             userID,
			RuleID:             ruleID,
			AlertEventStatusID: masters.OpenAlertStatusID,
			FiatWithdrawalID:   withdrawalID,
			TradeExecutionID:   sql.NullInt64{},
			Score:              "80.0000",
			DetectedAt:         time.Date(2031, 3, 14, 9, 25, 0, 0, time.UTC),
			Note:               "user-case-overview-alert",
		}).BuildForTest(t, ctx, tx)
		caseID := (&testdb.SuspiciousCaseBuilder{
			UserID:          userID,
			PublicCaseHash:  "9999999999999999999999999999999999999999999999999999999999999999",
			OpenedByTypeID:  masters.SystemActorTypeID,
			OpenedByID:      "integration-test-runner",
			SourceTypeID:    masters.AutoCaseSourceTypeID,
			AlertEventLogID: sql.NullInt64{Int64: alertID, Valid: true},
			Title:           "User case overview case",
			CurrentStatusID: masters.OpenCaseStatusID,
			RiskLevelID:     masters.HighRiskLevelID,
			OpenedAt:        time.Date(2031, 3, 14, 9, 30, 0, 0, time.UTC),
		}).BuildForTest(t, ctx, tx)
		(&testdb.AccountActionBuilder{
			UserID:           userID,
			SuspiciousCaseID: sql.NullInt64{Int64: caseID, Valid: true},
			ActionTypeID:     masters.FreezeActionTypeID,
			ActorTypeID:      masters.AdminActorTypeID,
			ActorID:          "admin-003",
			ActionReason:     "overview action",
			RequestedAt:      time.Date(2031, 3, 14, 9, 40, 0, 0, time.UTC),
			CompletedAt:      sql.NullTime{Time: time.Date(2031, 3, 14, 9, 50, 0, 0, time.UTC), Valid: true},
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/suspicious_transactions/user_case_overview.sql")
		defer rows.Close()

		// このSQLは alert_event_logs / suspicious_cases / account_actions を同時に LEFT JOIN している。
		// JOIN増幅が起きても件数が壊れないよう、COUNT(DISTINCT ...) の結果だけを検証する。
		var found bool
		for rows.Next() {
			var (
				userIDGot             int64
				memberCodeGot         string
				currentStatus         string
				alertCount            int64
				caseCount             int64
				accountActionCount    int64
				lastAlertAt           sql.NullTime
				lastCaseOpenedAt      sql.NullTime
				lastActionRequestedAt sql.NullTime
			)
			if err := rows.Scan(&userIDGot, &memberCodeGot, &currentStatus, &alertCount, &caseCount, &accountActionCount, &lastAlertAt, &lastCaseOpenedAt, &lastActionRequestedAt); err != nil {
				t.Fatalf("ユーザーケース概要の行読み取りに失敗しました: %v", err)
			}
			if userIDGot == userID {
				found = true
				if memberCodeGot != memberCode || currentStatus != "ACTIVE" {
					t.Fatalf("ユーザーケース概要の属性が期待値と異なります: memberCode=%s status=%s", memberCodeGot, currentStatus)
				}
				assertEqualInt64(t, alertCount, 1, "検知件数")
				assertEqualInt64(t, caseCount, 1, "ケース件数")
				assertEqualInt64(t, accountActionCount, 1, "口座措置件数")
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("ユーザーケース概要の走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("ユーザーケース概要に挿入したユーザー行が見つかりませんでした")
		}
	})
}

func TestAlertToCaseConversionTime(t *testing.T) {
	t.Run("検知からケース化までの時間集計_平均時間を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		userID := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID).BuildForTest(t, ctx, tx)

		withdrawalID := (&testdb.FiatWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "alert-to-case-withdrawal",
			CurrencyID:         masters.JPYCurrencyID,
			Amount:             "777777",
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        time.Date(2031, 3, 15, 9, 0, 0, 0, time.UTC),
			CompletedAt:        sql.NullTime{Time: time.Date(2031, 3, 15, 9, 10, 0, 0, time.UTC), Valid: true},
		}).BuildForTest(t, ctx, tx)
		ruleID := (&testdb.AlertRuleBuilder{
			PublicRuleHash: "1010101010101010101010101010101010101010101010101010101010101010",
			RuleName:       "Alert To Case Rule",
			RuleType:       "CONVERSION_TIME",
			Severity:       "CRITICAL",
			ThresholdJSON:  `{"sample":true}`,
		}).BuildForTest(t, ctx, tx)
		detectedAt := time.Date(2031, 3, 15, 9, 20, 0, 0, time.UTC)
		alertID := (&testdb.AlertEventLogBuilder{
			UserID:             userID,
			RuleID:             ruleID,
			AlertEventStatusID: masters.OpenAlertStatusID,
			FiatWithdrawalID:   withdrawalID,
			TradeExecutionID:   sql.NullInt64{},
			Score:              "99.9000",
			DetectedAt:         detectedAt,
			Note:               "alert-to-case-alert",
		}).BuildForTest(t, ctx, tx)
		(&testdb.SuspiciousCaseBuilder{
			UserID:          userID,
			PublicCaseHash:  "2020202020202020202020202020202020202020202020202020202020202020",
			OpenedByTypeID:  masters.SystemActorTypeID,
			OpenedByID:      "integration-test-runner",
			SourceTypeID:    masters.AutoCaseSourceTypeID,
			AlertEventLogID: sql.NullInt64{Int64: alertID, Valid: true},
			Title:           "Alert to case sample",
			CurrentStatusID: masters.OpenCaseStatusID,
			RiskLevelID:     masters.CriticalRiskLevelID,
			OpenedAt:        detectedAt.Add(30 * time.Minute),
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/suspicious_transactions/alert_to_case_conversion_time.sql")
		defer rows.Close()

		// detected_at から opened_at までを 30分差で投入している。
		// SQL側の
		//   TIMESTAMPDIFF(MINUTE, ael.detected_at, sc.opened_at)
		// の平均/最大/最小がすべて 30 になることを確認する。
		var found bool
		for rows.Next() {
			var (
				ruleName             string
				severity             string
				convertedCaseCount   int64
				avgConversionMinutes string
				maxConversionMinutes int64
				minConversionMinutes int64
			)
			if err := rows.Scan(&ruleName, &severity, &convertedCaseCount, &avgConversionMinutes, &maxConversionMinutes, &minConversionMinutes); err != nil {
				t.Fatalf("検知からケース化までの時間集計の行読み取りに失敗しました: %v", err)
			}
			if ruleName == "Alert To Case Rule" {
				found = true
				if severity != "CRITICAL" {
					t.Fatalf("重要度が期待値と異なります: expected=CRITICAL actual=%s", severity)
				}
				assertEqualInt64(t, convertedCaseCount, 1, "ケース化件数")
				if avgConversionMinutes != "30.00" || maxConversionMinutes != 30 || minConversionMinutes != 30 {
					t.Fatalf("ケース化時間が期待値と異なります: avg=%s max=%d min=%d", avgConversionMinutes, maxConversionMinutes, minConversionMinutes)
				}
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("検知からケース化までの時間集計の走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("検知からケース化までの時間集計に挿入したルール行が見つかりませんでした")
		}
	})
}

func TestDestinationAddressReuseCandidates(t *testing.T) {
	t.Run("出金先アドレス再利用候補_複数ユーザー共有アドレスを返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		userID1 := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID).BuildForTest(t, ctx, tx)
		userID2 := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID).BuildForTest(t, ctx, tx)
		sharedAddress := "integration_shared_address_001"
		requested1 := time.Date(2031, 3, 16, 9, 0, 0, 0, time.UTC)
		requested2 := time.Date(2031, 3, 16, 10, 0, 0, 0, time.UTC)

		(&testdb.CryptoWithdrawalBuilder{
			UserID:             userID1,
			PublicHash:         "destination-reuse-user1",
			CurrencyID:         masters.BTCurrencyID,
			DestinationAddress: sharedAddress,
			Amount:             "0.20000000",
			TxHash:             sql.NullString{String: "destination-reuse-user1-tx", Valid: true},
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        requested1,
			CompletedAt:        sql.NullTime{Time: requested1.Add(20 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)
		(&testdb.CryptoWithdrawalBuilder{
			UserID:             userID2,
			PublicHash:         "destination-reuse-user2",
			CurrencyID:         masters.BTCurrencyID,
			DestinationAddress: sharedAddress,
			Amount:             "0.30000000",
			TxHash:             sql.NullString{String: "destination-reuse-user2-tx", Valid: true},
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        requested2,
			CompletedAt:        sql.NullTime{Time: requested2.Add(20 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/suspicious_transactions/destination_address_reuse_candidates.sql")
		defer rows.Close()

		// 同一 destination_address を2ユーザーで共有しているデータを投入している。
		// SQLの HAVING COUNT(DISTINCT cw.user_id) >= 2 に引っかかり、
		// user_count=2 / withdrawal_count=2 が返ることを確認する。
		var found bool
		for rows.Next() {
			var (
				destinationAddress string
				currencyCode       string
				withdrawalCount    int64
				userCount          int64
				firstRequestedAt   time.Time
				lastRequestedAt    time.Time
			)
			if err := rows.Scan(&destinationAddress, &currencyCode, &withdrawalCount, &userCount, &firstRequestedAt, &lastRequestedAt); err != nil {
				t.Fatalf("出金先アドレス再利用候補の行読み取りに失敗しました: %v", err)
			}
			if destinationAddress == sharedAddress {
				found = true
				if currencyCode != "BTC" {
					t.Fatalf("通貨コードが期待値と異なります: expected=BTC actual=%s", currencyCode)
				}
				assertEqualInt64(t, withdrawalCount, 2, "出金件数")
				assertEqualInt64(t, userCount, 2, "ユーザー件数")
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("出金先アドレス再利用候補の走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("出金先アドレス再利用候補に共有アドレスの行が見つかりませんでした")
		}
	})
}

func TestUserBalanceReconciliationGap(t *testing.T) {
	t.Run("残高整合性確認_入出金と約定から理論残高増減を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		base := time.Date(2031, 3, 17, 9, 0, 0, 0, time.UTC)

		userBuilder := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID)
		memberCode := userBuilder.MemberCode
		userID := userBuilder.BuildForTest(t, ctx, tx)

		(&testdb.FiatDepositBuilder{
			UserID:          userID,
			PublicHash:      "balance-gap-fiat-deposit",
			CurrencyID:      masters.JPYCurrencyID,
			Amount:          "1000000",
			DepositStatusID: masters.CompletedDepositStatusID,
			RequestedAt:     base,
			CompletedAt:     sql.NullTime{Time: base.Add(5 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)

		orderID := (&testdb.TradingOrderBuilder{
			UserID:         userID,
			PublicHash:     "balance-gap-order",
			Side:           "BUY",
			OrderType:      "LIMIT",
			FromCurrencyID: masters.JPYCurrencyID,
			ToCurrencyID:   masters.BTCurrencyID,
			Price:          "500000",
			Quantity:       "1.00000000",
			OrderStatusID:  masters.FilledOrderStatusID,
			PlacedAt:       base.Add(10 * time.Minute),
		}).BuildForTest(t, ctx, tx)

		(&testdb.TradeExecutionBuilder{
			OrderID:          orderID,
			UserID:           userID,
			PublicHash:       "balance-gap-execution",
			FromCurrencyID:   masters.JPYCurrencyID,
			ToCurrencyID:     masters.BTCurrencyID,
			ExecutedPrice:    "500000",
			ExecutedQuantity: "1.00000000",
			FromAmount:       "500000.00000000",
			ToAmount:         "1.00000000",
			FeeCurrencyID:    masters.JPYCurrencyID,
			FeeAmount:        "1000",
			ExecutedAt:       base.Add(15 * time.Minute),
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/user_balance_reconciliation_gap.sql")
		defer rows.Close()

		var foundJPY bool
		var foundBTC bool
		for rows.Next() {
			var (
				userIDGot               int64
				memberCodeGot           string
				currencyCode            string
				firstEventAt            time.Time
				lastEventAt             time.Time
				externalNetAmount       string
				tradeNetAmount          string
				feeAmount               string
				theoreticalBalanceDelta string
			)
			if err := rows.Scan(&userIDGot, &memberCodeGot, &currencyCode, &firstEventAt, &lastEventAt, &externalNetAmount, &tradeNetAmount, &feeAmount, &theoreticalBalanceDelta); err != nil {
				t.Fatalf("残高整合性確認SQLの行読み取りに失敗しました: %v", err)
			}
			if userIDGot != userID {
				continue
			}
			if memberCodeGot != memberCode {
				t.Fatalf("会員コードが期待値と異なります: expected=%s actual=%s", memberCode, memberCodeGot)
			}
			if currencyCode == "JPY" {
				foundJPY = true
				if externalNetAmount != "1000000.000000000000000000" || tradeNetAmount != "-500000.000000000000000000" || feeAmount != "1000.000000000000000000" || theoreticalBalanceDelta != "499000.000000000000000000" {
					t.Fatalf("JPY の理論残高増減が期待値と異なります: external=%s trade=%s fee=%s delta=%s", externalNetAmount, tradeNetAmount, feeAmount, theoreticalBalanceDelta)
				}
			}
			if currencyCode == "BTC" {
				foundBTC = true
				if externalNetAmount != "0.000000000000000000" || tradeNetAmount != "1.000000000000000000" || feeAmount != "0.000000000000000000" || theoreticalBalanceDelta != "1.000000000000000000" {
					t.Fatalf("BTC の理論残高増減が期待値と異なります: external=%s trade=%s fee=%s delta=%s", externalNetAmount, tradeNetAmount, feeAmount, theoreticalBalanceDelta)
				}
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("残高整合性確認SQLの走査中に失敗しました: %v", err)
		}
		if !foundJPY || !foundBTC {
			t.Fatalf("残高整合性確認SQLに期待した通貨行が見つかりませんでした: found_jpy=%t found_btc=%t", foundJPY, foundBTC)
		}
	})
}

func TestStuckPendingTransactions(t *testing.T) {
	t.Run("滞留中トランザクション一覧_長時間PENDINGの入出金を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		now := currentDBTime(t, ctx, tx)

		userID := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID).BuildForTest(t, ctx, tx)

		(&testdb.FiatDepositBuilder{
			UserID:          userID,
			PublicHash:      "stuck-pending-fiat-deposit",
			CurrencyID:      masters.JPYCurrencyID,
			Amount:          "500000",
			DepositStatusID: masters.PendingDepositStatusID,
			RequestedAt:     now.Add(-3 * time.Hour),
			CompletedAt:     sql.NullTime{},
		}).BuildForTest(t, ctx, tx)
		cryptoWithdrawalID := (&testdb.CryptoWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "stuck-pending-crypto-withdrawal",
			CurrencyID:         masters.BTCurrencyID,
			DestinationAddress: "stuck_pending_btc_address",
			Amount:             "1.25000000",
			TxHash:             sql.NullString{},
			WithdrawalStatusID: masters.PendingWithdrawalID,
			RequestedAt:        now.Add(-2 * time.Hour),
			CompletedAt:        sql.NullTime{},
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/stuck_pending_transactions.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				operationType  string
				transactionID  int64
				userIDGot      int64
				memberCode     string
				currencyCode   string
				statusValue    string
				startedAt      time.Time
				pendingMinutes int64
				amount         string
			)
			if err := rows.Scan(&operationType, &transactionID, &userIDGot, &memberCode, &currencyCode, &statusValue, &startedAt, &pendingMinutes, &amount); err != nil {
				t.Fatalf("滞留中トランザクション一覧の行読み取りに失敗しました: %v", err)
			}
			_ = startedAt
			if userIDGot == userID && operationType == "CRYPTO_WITHDRAWAL" && transactionID == cryptoWithdrawalID {
				found = true
				if currencyCode != "BTC" || statusValue != "PENDING" {
					t.Fatalf("滞留中トランザクションの属性が期待値と異なります: currency=%s status=%s memberCode=%s", currencyCode, statusValue, memberCode)
				}
				assertGreaterOrEqualInt64(t, pendingMinutes, 120, "滞留分数")
				if amount != "1.250000000000000000" {
					t.Fatalf("滞留中トランザクションの数量が期待値と異なります: expected=1.250000000000000000 actual=%s", amount)
				}
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("滞留中トランザクション一覧の走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("滞留中トランザクション一覧に挿入した PENDING 出金が見つかりませんでした")
		}
	})
}

func TestHighRiskUserActivitySummary(t *testing.T) {
	t.Run("高リスクユーザー活動集計_取引と検知と措置を1行で返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		base := time.Date(2031, 3, 18, 9, 0, 0, 0, time.UTC)

		userBuilder := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID)
		memberCode := userBuilder.MemberCode
		userID := userBuilder.BuildForTest(t, ctx, tx)

		(&testdb.FiatDepositBuilder{
			UserID:          userID,
			PublicHash:      "high-risk-fiat-deposit",
			CurrencyID:      masters.JPYCurrencyID,
			Amount:          "750000",
			DepositStatusID: masters.CompletedDepositStatusID,
			RequestedAt:     base,
			CompletedAt:     sql.NullTime{Time: base.Add(10 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)
		withdrawalID := (&testdb.FiatWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "high-risk-fiat-withdrawal",
			CurrencyID:         masters.JPYCurrencyID,
			Amount:             "650000",
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        base.Add(20 * time.Minute),
			CompletedAt:        sql.NullTime{Time: base.Add(30 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)
		orderID := (&testdb.TradingOrderBuilder{
			UserID:         userID,
			PublicHash:     "high-risk-order",
			Side:           "BUY",
			OrderType:      "LIMIT",
			FromCurrencyID: masters.JPYCurrencyID,
			ToCurrencyID:   masters.BTCurrencyID,
			Price:          "300000",
			Quantity:       "0.50000000",
			OrderStatusID:  masters.FilledOrderStatusID,
			PlacedAt:       base.Add(40 * time.Minute),
		}).BuildForTest(t, ctx, tx)
		(&testdb.TradeExecutionBuilder{
			OrderID:          orderID,
			UserID:           userID,
			PublicHash:       "high-risk-execution",
			FromCurrencyID:   masters.JPYCurrencyID,
			ToCurrencyID:     masters.BTCurrencyID,
			ExecutedPrice:    "300000",
			ExecutedQuantity: "0.50000000",
			FeeCurrencyID:    masters.JPYCurrencyID,
			FeeAmount:        "500",
			ExecutedAt:       base.Add(45 * time.Minute),
		}).BuildForTest(t, ctx, tx)
		ruleID := (&testdb.AlertRuleBuilder{
			PublicRuleHash: "3030303030303030303030303030303030303030303030303030303030303030",
			RuleName:       "High Risk Summary Rule",
			RuleType:       "HIGH_RISK_SUMMARY",
			Severity:       "CRITICAL",
			ThresholdJSON:  `{"sample":true}`,
		}).BuildForTest(t, ctx, tx)
		alertID := (&testdb.AlertEventLogBuilder{
			UserID:             userID,
			RuleID:             ruleID,
			AlertEventStatusID: masters.OpenAlertStatusID,
			FiatWithdrawalID:   withdrawalID,
			TradeExecutionID:   sql.NullInt64{},
			Score:              "97.1000",
			DetectedAt:         base.Add(50 * time.Minute),
			Note:               "high-risk-summary-alert",
		}).BuildForTest(t, ctx, tx)
		caseID := (&testdb.SuspiciousCaseBuilder{
			UserID:          userID,
			PublicCaseHash:  "4040404040404040404040404040404040404040404040404040404040404040",
			OpenedByTypeID:  masters.SystemActorTypeID,
			OpenedByID:      "integration-test-runner",
			SourceTypeID:    masters.AutoCaseSourceTypeID,
			AlertEventLogID: sql.NullInt64{Int64: alertID, Valid: true},
			Title:           "High risk summary case",
			CurrentStatusID: masters.InvestigatingCaseStatusID,
			RiskLevelID:     masters.CriticalRiskLevelID,
			OpenedAt:        base.Add(55 * time.Minute),
		}).BuildForTest(t, ctx, tx)
		(&testdb.AccountActionBuilder{
			UserID:           userID,
			SuspiciousCaseID: sql.NullInt64{Int64: caseID, Valid: true},
			ActionTypeID:     masters.FreezeActionTypeID,
			ActorTypeID:      masters.AdminActorTypeID,
			ActorID:          "admin-high-risk",
			ActionReason:     "high risk summary action",
			RequestedAt:      base.Add(60 * time.Minute),
			CompletedAt:      sql.NullTime{Time: base.Add(70 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/suspicious_transactions/high_risk_user_activity_summary.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				userIDGot           int64
				memberCodeGot       string
				currentStatus       string
				maxRiskLevel        string
				caseCount           int64
				openCaseCount       int64
				executionCount      int64
				fiatDepositCount    int64
				fiatWithdrawalCount int64
				alertCount          int64
				actionCount         int64
				lastCaseOpenedAt    sql.NullTime
				lastAlertAt         sql.NullTime
			)
			if err := rows.Scan(&userIDGot, &memberCodeGot, &currentStatus, &maxRiskLevel, &caseCount, &openCaseCount, &executionCount, &fiatDepositCount, &fiatWithdrawalCount, &alertCount, &actionCount, &lastCaseOpenedAt, &lastAlertAt); err != nil {
				t.Fatalf("高リスクユーザー活動集計の行読み取りに失敗しました: %v", err)
			}
			_ = lastCaseOpenedAt
			_ = lastAlertAt
			if userIDGot == userID {
				found = true
				if memberCodeGot != memberCode || currentStatus != "ACTIVE" || maxRiskLevel != "CRITICAL" {
					t.Fatalf("高リスクユーザー活動集計の属性が期待値と異なります: memberCode=%s status=%s risk=%s", memberCodeGot, currentStatus, maxRiskLevel)
				}
				assertEqualInt64(t, caseCount, 1, "ケース件数")
				assertEqualInt64(t, openCaseCount, 1, "未クローズケース件数")
				assertEqualInt64(t, executionCount, 1, "約定件数")
				assertEqualInt64(t, fiatDepositCount, 1, "法定入金件数")
				assertEqualInt64(t, fiatWithdrawalCount, 1, "法定出金件数")
				assertEqualInt64(t, alertCount, 1, "検知件数")
				assertEqualInt64(t, actionCount, 1, "措置件数")
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("高リスクユーザー活動集計の走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("高リスクユーザー活動集計に挿入したユーザー行が見つかりませんでした")
		}
	})
}

func TestAlertRepeatUserSummary(t *testing.T) {
	t.Run("繰り返し検知ユーザー集計_複数回検知されたユーザーを返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		base := time.Date(2031, 3, 19, 9, 0, 0, 0, time.UTC)

		userBuilder := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID)
		memberCode := userBuilder.MemberCode
		userID := userBuilder.BuildForTest(t, ctx, tx)

		withdrawalID1 := (&testdb.FiatWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "repeat-alert-withdrawal-1",
			CurrencyID:         masters.JPYCurrencyID,
			Amount:             "600000",
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        base,
			CompletedAt:        sql.NullTime{Time: base.Add(10 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)
		withdrawalID2 := (&testdb.FiatWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "repeat-alert-withdrawal-2",
			CurrencyID:         masters.JPYCurrencyID,
			Amount:             "620000",
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        base.Add(30 * time.Minute),
			CompletedAt:        sql.NullTime{Time: base.Add(40 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)
		ruleID1 := (&testdb.AlertRuleBuilder{
			PublicRuleHash: "5050505050505050505050505050505050505050505050505050505050505050",
			RuleName:       "Repeat Alert Rule 1",
			RuleType:       "REPEAT_1",
			Severity:       "HIGH",
			ThresholdJSON:  `{"sample":1}`,
		}).BuildForTest(t, ctx, tx)
		ruleID2 := (&testdb.AlertRuleBuilder{
			PublicRuleHash: "6060606060606060606060606060606060606060606060606060606060606060",
			RuleName:       "Repeat Alert Rule 2",
			RuleType:       "REPEAT_2",
			Severity:       "CRITICAL",
			ThresholdJSON:  `{"sample":2}`,
		}).BuildForTest(t, ctx, tx)
		alertID := (&testdb.AlertEventLogBuilder{
			UserID:             userID,
			RuleID:             ruleID1,
			AlertEventStatusID: masters.OpenAlertStatusID,
			FiatWithdrawalID:   withdrawalID1,
			TradeExecutionID:   sql.NullInt64{},
			Score:              "80.0000",
			DetectedAt:         base.Add(12 * time.Minute),
			Note:               "repeat-alert-1",
		}).BuildForTest(t, ctx, tx)
		(&testdb.AlertEventLogBuilder{
			UserID:             userID,
			RuleID:             ruleID2,
			AlertEventStatusID: masters.ReviewedAlertStatusID,
			FiatWithdrawalID:   withdrawalID2,
			TradeExecutionID:   sql.NullInt64{},
			Score:              "90.0000",
			DetectedAt:         base.Add(42 * time.Minute),
			Note:               "repeat-alert-2",
		}).BuildForTest(t, ctx, tx)
		(&testdb.SuspiciousCaseBuilder{
			UserID:          userID,
			PublicCaseHash:  "7070707070707070707070707070707070707070707070707070707070707070",
			OpenedByTypeID:  masters.SystemActorTypeID,
			OpenedByID:      "integration-test-runner",
			SourceTypeID:    masters.AutoCaseSourceTypeID,
			AlertEventLogID: sql.NullInt64{Int64: alertID, Valid: true},
			Title:           "repeat alert case",
			CurrentStatusID: masters.OpenCaseStatusID,
			RiskLevelID:     masters.HighRiskLevelID,
			OpenedAt:        base.Add(20 * time.Minute),
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/suspicious_transactions/alert_repeat_user_summary.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				userIDGot         int64
				memberCodeGot     string
				detectionCount    int64
				distinctRuleCount int64
				linkedCaseCount   int64
				firstDetectedAt   time.Time
				lastDetectedAt    time.Time
			)
			if err := rows.Scan(&userIDGot, &memberCodeGot, &detectionCount, &distinctRuleCount, &linkedCaseCount, &firstDetectedAt, &lastDetectedAt); err != nil {
				t.Fatalf("繰り返し検知ユーザー集計の行読み取りに失敗しました: %v", err)
			}
			_ = firstDetectedAt
			_ = lastDetectedAt
			if userIDGot == userID {
				found = true
				if memberCodeGot != memberCode {
					t.Fatalf("会員コードが期待値と異なります: expected=%s actual=%s", memberCode, memberCodeGot)
				}
				assertEqualInt64(t, detectionCount, 2, "検知件数")
				assertEqualInt64(t, distinctRuleCount, 2, "ルール件数")
				assertEqualInt64(t, linkedCaseCount, 1, "ケース連携件数")
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("繰り返し検知ユーザー集計の走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("繰り返し検知ユーザー集計に挿入したユーザー行が見つかりませんでした")
		}
	})
}

func TestStatusChangeAfterAlert(t *testing.T) {
	t.Run("アラート後ステータス変更一覧_検知後の凍結イベントを返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		base := time.Date(2031, 3, 20, 9, 0, 0, 0, time.UTC)

		userBuilder := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID)
		memberCode := userBuilder.MemberCode
		userID := userBuilder.BuildForTest(t, ctx, tx)

		withdrawalID := (&testdb.FiatWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "status-change-after-alert-withdrawal",
			CurrencyID:         masters.JPYCurrencyID,
			Amount:             "700000",
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        base,
			CompletedAt:        sql.NullTime{Time: base.Add(10 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)
		ruleID := (&testdb.AlertRuleBuilder{
			PublicRuleHash: "8080808080808080808080808080808080808080808080808080808080808080",
			RuleName:       "Status Change After Alert Rule",
			RuleType:       "STATUS_AFTER_ALERT",
			Severity:       "CRITICAL",
			ThresholdJSON:  `{"sample":true}`,
		}).BuildForTest(t, ctx, tx)
		detectedAt := base.Add(15 * time.Minute)
		(&testdb.AlertEventLogBuilder{
			UserID:             userID,
			RuleID:             ruleID,
			AlertEventStatusID: masters.OpenAlertStatusID,
			FiatWithdrawalID:   withdrawalID,
			TradeExecutionID:   sql.NullInt64{},
			Score:              "95.0000",
			DetectedAt:         detectedAt,
			Note:               "status-change-after-alert",
		}).BuildForTest(t, ctx, tx)
		statusEventID := (&testdb.UserStatusChangeEventBuilder{
			UserID:      userID,
			EventTypeID: masters.FrozenEventTypeID,
			ActorTypeID: masters.SystemActorTypeID,
			ActorID:     "status-after-alert-batch",
			Reason:      "freeze after critical alert",
			OccurredAt:  detectedAt.Add(5 * time.Minute),
		}).BuildForTest(t, ctx, tx)
		(&testdb.UserStatusHistoryBuilder{
			UserID:              userID,
			StatusChangeEventID: statusEventID,
			FromStatusID:        sql.NullInt64{Int64: masters.ActiveUserStatusID, Valid: true},
			ToStatusID:          masters.FrozenUserStatusID,
			ChangedAt:           detectedAt.Add(5 * time.Minute),
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/suspicious_transactions/status_change_after_alert.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				alertEventID    int64
				userIDGot       int64
				memberCodeGot   string
				ruleName        string
				detectedAtGot   time.Time
				statusEventType string
				fromStatus      sql.NullString
				toStatus        sql.NullString
				reason          string
				statusChangedAt time.Time
				delayMinutes    int64
			)
			if err := rows.Scan(&alertEventID, &userIDGot, &memberCodeGot, &ruleName, &detectedAtGot, &statusEventType, &fromStatus, &toStatus, &reason, &statusChangedAt, &delayMinutes); err != nil {
				t.Fatalf("アラート後ステータス変更一覧の行読み取りに失敗しました: %v", err)
			}
			_ = alertEventID
			_ = detectedAtGot
			_ = statusChangedAt
			if userIDGot == userID {
				found = true
				if memberCodeGot != memberCode || ruleName != "Status Change After Alert Rule" || statusEventType != "FROZEN" {
					t.Fatalf("アラート後ステータス変更一覧の属性が期待値と異なります: memberCode=%s rule=%s type=%s", memberCodeGot, ruleName, statusEventType)
				}
				if !fromStatus.Valid || fromStatus.String != "ACTIVE" || !toStatus.Valid || toStatus.String != "FROZEN" {
					t.Fatalf("アラート後ステータス変更一覧のステータス遷移が期待値と異なります: from=%v to=%v reason=%s", fromStatus, toStatus, reason)
				}
				assertEqualInt64(t, delayMinutes, 5, "検知後ステータス変更までの分数")
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("アラート後ステータス変更一覧の走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("アラート後ステータス変更一覧に挿入した行が見つかりませんでした")
		}
	})
}

func TestLargeUnmatchedCryptoInflow(t *testing.T) {
	t.Run("高額未対応暗号資産入金一覧_売却も出金もない大口入金を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		userBuilder := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID)
		memberCode := userBuilder.MemberCode
		userID := userBuilder.BuildForTest(t, ctx, tx)
		base := time.Date(2031, 3, 21, 9, 0, 0, 0, time.UTC)

		cryptoDepositID := (&testdb.CryptoDepositBuilder{
			UserID:          userID,
			PublicHash:      "large-unmatched-crypto-deposit",
			CurrencyID:      masters.BTCurrencyID,
			TxHash:          "large-unmatched-crypto-deposit-tx",
			Amount:          "2.50000000",
			DepositStatusID: masters.CompletedDepositStatusID,
			DetectedAt:      base,
			ConfirmedAt:     sql.NullTime{Time: base.Add(30 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/suspicious_transactions/large_unmatched_crypto_inflow.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				cryptoDepositIDGot        int64
				userIDGot                 int64
				memberCodeGot             string
				currencyCode              string
				amount                    string
				confirmedAt               time.Time
				matchedSellExecutionCount int64
				matchedWithdrawalCount    int64
			)
			if err := rows.Scan(&cryptoDepositIDGot, &userIDGot, &memberCodeGot, &currencyCode, &amount, &confirmedAt, &matchedSellExecutionCount, &matchedWithdrawalCount); err != nil {
				t.Fatalf("高額未対応暗号資産入金一覧の行読み取りに失敗しました: %v", err)
			}
			_ = confirmedAt
			if cryptoDepositIDGot == cryptoDepositID {
				found = true
				if userIDGot != userID || memberCodeGot != memberCode || currencyCode != "BTC" {
					t.Fatalf("高額未対応暗号資産入金一覧の属性が期待値と異なります: userID=%d memberCode=%s currency=%s", userIDGot, memberCodeGot, currencyCode)
				}
				if amount != "2.500000000000000000" {
					t.Fatalf("高額未対応暗号資産入金一覧の数量が期待値と異なります: expected=2.500000000000000000 actual=%s", amount)
				}
				assertEqualInt64(t, matchedSellExecutionCount, 0, "対応する売却約定件数")
				assertEqualInt64(t, matchedWithdrawalCount, 0, "対応する出金件数")
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("高額未対応暗号資産入金一覧の走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("高額未対応暗号資産入金一覧に挿入した入金行が見つかりませんでした")
		}
	})
}

func TestUserAlertCaseTimeline(t *testing.T) {
	t.Run("ユーザーイベントタイムライン_検知から措置までを時系列で返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		base := time.Date(2031, 3, 22, 9, 0, 0, 0, time.UTC)

		userBuilder := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID)
		memberCode := userBuilder.MemberCode
		userID := userBuilder.BuildForTest(t, ctx, tx)

		withdrawalID := (&testdb.FiatWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "timeline-withdrawal",
			CurrencyID:         masters.JPYCurrencyID,
			Amount:             "800000",
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        base,
			CompletedAt:        sql.NullTime{Time: base.Add(10 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)
		ruleID := (&testdb.AlertRuleBuilder{
			PublicRuleHash: "9090909090909090909090909090909090909090909090909090909090909090",
			RuleName:       "Timeline Rule",
			RuleType:       "TIMELINE",
			Severity:       "HIGH",
			ThresholdJSON:  `{"sample":true}`,
		}).BuildForTest(t, ctx, tx)
		alertAt := base.Add(15 * time.Minute)
		alertID := (&testdb.AlertEventLogBuilder{
			UserID:             userID,
			RuleID:             ruleID,
			AlertEventStatusID: masters.OpenAlertStatusID,
			FiatWithdrawalID:   withdrawalID,
			TradeExecutionID:   sql.NullInt64{},
			Score:              "88.8000",
			DetectedAt:         alertAt,
			Note:               "timeline-alert",
		}).BuildForTest(t, ctx, tx)
		caseID := (&testdb.SuspiciousCaseBuilder{
			UserID:          userID,
			PublicCaseHash:  "9191919191919191919191919191919191919191919191919191919191919191",
			OpenedByTypeID:  masters.SystemActorTypeID,
			OpenedByID:      "integration-test-runner",
			SourceTypeID:    masters.AutoCaseSourceTypeID,
			AlertEventLogID: sql.NullInt64{Int64: alertID, Valid: true},
			Title:           "timeline case",
			CurrentStatusID: masters.InvestigatingCaseStatusID,
			RiskLevelID:     masters.HighRiskLevelID,
			AssignedTo:      sql.NullString{String: "aml-operator-timeline", Valid: true},
			OpenedAt:        base.Add(20 * time.Minute),
		}).BuildForTest(t, ctx, tx)
		(&testdb.AccountActionBuilder{
			UserID:           userID,
			SuspiciousCaseID: sql.NullInt64{Int64: caseID, Valid: true},
			ActionTypeID:     masters.FreezeActionTypeID,
			ActorTypeID:      masters.AdminActorTypeID,
			ActorID:          "timeline-admin",
			ActionReason:     "timeline freeze",
			RequestedAt:      base.Add(25 * time.Minute),
			CompletedAt:      sql.NullTime{Time: base.Add(30 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)
		(&testdb.UserStatusChangeEventBuilder{
			UserID:      userID,
			EventTypeID: masters.FrozenEventTypeID,
			ActorTypeID: masters.SystemActorTypeID,
			ActorID:     "timeline-batch",
			Reason:      "timeline status change",
			OccurredAt:  base.Add(35 * time.Minute),
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/suspicious_transactions/user_alert_case_timeline.sql")
		defer rows.Close()

		foundTypes := map[string]bool{}
		for rows.Next() {
			var (
				userIDGot      int64
				memberCodeGot  string
				eventAt        time.Time
				eventType      string
				eventID        int64
				primaryLabel   string
				secondaryLabel string
			)
			if err := rows.Scan(&userIDGot, &memberCodeGot, &eventAt, &eventType, &eventID, &primaryLabel, &secondaryLabel); err != nil {
				t.Fatalf("ユーザーイベントタイムラインの行読み取りに失敗しました: %v", err)
			}
			_ = eventAt
			_ = eventID
			_ = primaryLabel
			_ = secondaryLabel
			if userIDGot == userID {
				if memberCodeGot != memberCode {
					t.Fatalf("会員コードが期待値と異なります: expected=%s actual=%s", memberCode, memberCodeGot)
				}
				foundTypes[eventType] = true
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("ユーザーイベントタイムラインの走査中に失敗しました: %v", err)
		}
		for _, eventType := range []string{"ALERT_DETECTED", "CASE_OPENED", "ACCOUNT_ACTION", "USER_STATUS_CHANGED"} {
			if !foundTypes[eventType] {
				t.Fatalf("ユーザーイベントタイムラインに期待したイベント種別が見つかりませんでした: eventType=%s", eventType)
			}
		}
	})
}

func TestPendingTransactionBacklogSummary(t *testing.T) {
	t.Run("滞留中トランザクション集計_通貨別滞留帯別の件数を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		now := currentDBTime(t, ctx, tx)
		userID := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID).BuildForTest(t, ctx, tx)

		(&testdb.FiatDepositBuilder{
			UserID:          userID,
			PublicHash:      "backlog-pending-fiat-deposit",
			CurrencyID:      masters.JPYCurrencyID,
			Amount:          "123456",
			DepositStatusID: masters.PendingDepositStatusID,
			RequestedAt:     now.Add(-26 * time.Hour),
			CompletedAt:     sql.NullTime{},
		}).BuildForTest(t, ctx, tx)
		(&testdb.CryptoWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "backlog-pending-crypto-withdrawal",
			CurrencyID:         masters.BTCurrencyID,
			DestinationAddress: "backlog_pending_btc",
			Amount:             "0.50000000",
			TxHash:             sql.NullString{},
			WithdrawalStatusID: masters.PendingWithdrawalID,
			RequestedAt:        now.Add(-2 * time.Hour),
			CompletedAt:        sql.NullTime{},
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/pending_transaction_backlog_summary.sql")
		defer rows.Close()

		var foundOver24 bool
		var foundUnder24 bool
		for rows.Next() {
			var (
				operationType     string
				currencyCode      string
				backlogBucket     string
				backlogCount      int64
				oldestStartedAt   time.Time
				maxPendingMinutes int64
			)
			if err := rows.Scan(&operationType, &currencyCode, &backlogBucket, &backlogCount, &oldestStartedAt, &maxPendingMinutes); err != nil {
				t.Fatalf("滞留中トランザクション集計の行読み取りに失敗しました: %v", err)
			}
			_ = oldestStartedAt
			if operationType == "FIAT_DEPOSIT" && currencyCode == "JPY" && backlogBucket == "OVER_24_HOURS" {
				foundOver24 = true
				assertGreaterOrEqualInt64(t, backlogCount, 1, "24時間超滞留件数")
				assertGreaterOrEqualInt64(t, maxPendingMinutes, 24*60, "24時間超滞留分数")
			}
			if operationType == "CRYPTO_WITHDRAWAL" && currencyCode == "BTC" && backlogBucket == "UNDER_24_HOURS" {
				foundUnder24 = true
				assertGreaterOrEqualInt64(t, backlogCount, 1, "24時間以内滞留件数")
				assertGreaterOrEqualInt64(t, maxPendingMinutes, 120, "24時間以内滞留分数")
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("滞留中トランザクション集計の走査中に失敗しました: %v", err)
		}
		if !foundOver24 || !foundUnder24 {
			t.Fatalf("滞留中トランザクション集計に期待した集計行が見つかりませんでした: over24=%t under24=%t", foundOver24, foundUnder24)
		}
	})
}

func TestSuspiciousRapidOutflowCandidatesProcedural(t *testing.T) {
	t.Run("疑わしい取引手続き版_急速出金候補に優先度と理由を付けて返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)

		userID := testdb.NewUserBuilder().
			WithStatusID(masters.ActiveUserStatusID).
			BuildForTest(t, ctx, tx)

		depositCompletedAt := time.Date(2031, 4, 1, 9, 5, 0, 0, time.UTC)
		depositID := (&testdb.FiatDepositBuilder{
			UserID:          userID,
			PublicHash:      "rapid-outflow-procedural-deposit-test",
			CurrencyID:      masters.JPYCurrencyID,
			Amount:          "1000000",
			DepositStatusID: masters.CompletedDepositStatusID,
			RequestedAt:     depositCompletedAt.Add(-5 * time.Minute),
			CompletedAt:     sql.NullTime{Time: depositCompletedAt, Valid: true},
		}).BuildForTest(t, ctx, tx)

		(&testdb.FiatWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "rapid-outflow-procedural-withdrawal-test",
			CurrencyID:         masters.JPYCurrencyID,
			Amount:             "970000",
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        depositCompletedAt.Add(2 * time.Hour),
			CompletedAt:        sql.NullTime{Time: depositCompletedAt.Add(2*time.Hour + 15*time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/suspicious_transactions/suspicious_rapid_outflow_candidates_procedural.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				userIDGot            int64
				memberCode           string
				currencyCode         string
				inflowType           string
				inflowID             int64
				inflowCompletedAtGot time.Time
				inflowAmount         string
				matchedCount         int64
				matchedAmount        string
				outflowRatio         string
				reviewPriority       string
				reviewReason         string
			)
			if err := rows.Scan(
				&userIDGot,
				&memberCode,
				&currencyCode,
				&inflowType,
				&inflowID,
				&inflowCompletedAtGot,
				&inflowAmount,
				&matchedCount,
				&matchedAmount,
				&outflowRatio,
				&reviewPriority,
				&reviewReason,
			); err != nil {
				t.Fatalf("疑わしい取引手続き版の行読み取りに失敗しました: %v", err)
			}
			_ = memberCode
			_ = inflowCompletedAtGot
			_ = inflowAmount
			_ = matchedAmount
			if userIDGot == userID {
				found = true
				if currencyCode != "JPY" || inflowType != "FIAT_DEPOSIT" || inflowID != depositID {
					t.Fatalf("疑わしい取引手続き版のキー項目が期待値と異なります: currency=%s inflowType=%s inflowID=%d", currencyCode, inflowType, inflowID)
				}
				assertEqualInt64(t, matchedCount, 1, "手続き版の対応する出金件数")
				if outflowRatio != "0.9700" || reviewPriority != "CRITICAL" {
					t.Fatalf("疑わしい取引手続き版の判定結果が期待値と異なります: ratio=%s priority=%s", outflowRatio, reviewPriority)
				}
				if reviewReason != "入金の95%以上が24時間以内に流出" {
					t.Fatalf("疑わしい取引手続き版の理由が期待値と異なります: actual=%s", reviewReason)
				}
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("疑わしい取引手続き版の走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("疑わしい取引手続き版に挿入した行が見つかりませんでした")
		}
	})
}

func TestUserBalanceReconciliationGapProcedural(t *testing.T) {
	t.Run("残高整合性確認手続き版_重要度と調査要否を付けて返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		base := time.Date(2031, 4, 2, 9, 0, 0, 0, time.UTC)

		userBuilder := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID)
		memberCode := userBuilder.MemberCode
		userID := userBuilder.BuildForTest(t, ctx, tx)

		(&testdb.FiatDepositBuilder{
			UserID:          userID,
			PublicHash:      "balance-gap-procedural-fiat-deposit",
			CurrencyID:      masters.JPYCurrencyID,
			Amount:          "1000000",
			DepositStatusID: masters.CompletedDepositStatusID,
			RequestedAt:     base,
			CompletedAt:     sql.NullTime{Time: base.Add(5 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)

		orderID := (&testdb.TradingOrderBuilder{
			UserID:         userID,
			PublicHash:     "balance-gap-procedural-order",
			Side:           "BUY",
			OrderType:      "LIMIT",
			FromCurrencyID: masters.JPYCurrencyID,
			ToCurrencyID:   masters.BTCurrencyID,
			Price:          "500000",
			Quantity:       "1.00000000",
			OrderStatusID:  masters.FilledOrderStatusID,
			PlacedAt:       base.Add(10 * time.Minute),
		}).BuildForTest(t, ctx, tx)

		(&testdb.TradeExecutionBuilder{
			OrderID:          orderID,
			UserID:           userID,
			PublicHash:       "balance-gap-procedural-execution",
			FromCurrencyID:   masters.JPYCurrencyID,
			ToCurrencyID:     masters.BTCurrencyID,
			ExecutedPrice:    "500000",
			ExecutedQuantity: "1.00000000",
			FromAmount:       "500000.00000000",
			ToAmount:         "1.00000000",
			FeeCurrencyID:    masters.JPYCurrencyID,
			FeeAmount:        "1000",
			ExecutedAt:       base.Add(15 * time.Minute),
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/user_balance_reconciliation_gap_procedural.sql")
		defer rows.Close()

		var foundJPY bool
		var foundBTC bool
		for rows.Next() {
			var (
				userIDGot               int64
				memberCodeGot           string
				currencyCode            string
				firstEventAt            time.Time
				lastEventAt             time.Time
				externalNetAmount       string
				tradeNetAmount          string
				feeAmount               string
				theoreticalBalanceDelta string
				severityLabel           string
				needsInvestigation      int64
			)
			if err := rows.Scan(&userIDGot, &memberCodeGot, &currencyCode, &firstEventAt, &lastEventAt, &externalNetAmount, &tradeNetAmount, &feeAmount, &theoreticalBalanceDelta, &severityLabel, &needsInvestigation); err != nil {
				t.Fatalf("残高整合性確認手続き版の行読み取りに失敗しました: %v", err)
			}
			_ = firstEventAt
			_ = lastEventAt
			if userIDGot != userID {
				continue
			}
			if memberCodeGot != memberCode {
				t.Fatalf("残高整合性確認手続き版の会員コードが期待値と異なります: expected=%s actual=%s", memberCode, memberCodeGot)
			}
			if currencyCode == "JPY" {
				foundJPY = true
				if externalNetAmount != "1000000.000000000000000000" || tradeNetAmount != "-500000.000000000000000000" || feeAmount != "1000.000000000000000000" || theoreticalBalanceDelta != "499000.000000000000000000" {
					t.Fatalf("残高整合性確認手続き版のJPY差分が期待値と異なります: external=%s trade=%s fee=%s delta=%s", externalNetAmount, tradeNetAmount, feeAmount, theoreticalBalanceDelta)
				}
				if severityLabel != "HIGH" || needsInvestigation != 1 {
					t.Fatalf("残高整合性確認手続き版のJPY判定が期待値と異なります: severity=%s needs=%d", severityLabel, needsInvestigation)
				}
			}
			if currencyCode == "BTC" {
				foundBTC = true
				if theoreticalBalanceDelta != "1.000000000000000000" || severityLabel != "NORMAL" || needsInvestigation != 0 {
					t.Fatalf("残高整合性確認手続き版のBTC判定が期待値と異なります: delta=%s severity=%s needs=%d", theoreticalBalanceDelta, severityLabel, needsInvestigation)
				}
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("残高整合性確認手続き版の走査中に失敗しました: %v", err)
		}
		if !foundJPY || !foundBTC {
			t.Fatalf("残高整合性確認手続き版に期待した通貨行が見つかりませんでした: jpy=%t btc=%t", foundJPY, foundBTC)
		}
	})
}

func TestStatusChangeAfterAlertProcedural(t *testing.T) {
	t.Run("アラート後ステータス変更手続き版_最初の措置と応答帯を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		base := time.Date(2031, 4, 3, 9, 0, 0, 0, time.UTC)

		userBuilder := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID)
		memberCode := userBuilder.MemberCode
		userID := userBuilder.BuildForTest(t, ctx, tx)

		withdrawalID := (&testdb.FiatWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "status-change-after-alert-procedural-withdrawal",
			CurrencyID:         masters.JPYCurrencyID,
			Amount:             "700000",
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        base,
			CompletedAt:        sql.NullTime{Time: base.Add(10 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)
		ruleID := (&testdb.AlertRuleBuilder{
			PublicRuleHash: "8181818181818181818181818181818181818181818181818181818181818181",
			RuleName:       "Status Change After Alert Procedural Rule",
			RuleType:       "STATUS_AFTER_ALERT_PROCEDURAL",
			Severity:       "CRITICAL",
			ThresholdJSON:  `{"sample":true}`,
		}).BuildForTest(t, ctx, tx)
		detectedAt := base.Add(15 * time.Minute)
		(&testdb.AlertEventLogBuilder{
			UserID:             userID,
			RuleID:             ruleID,
			AlertEventStatusID: masters.OpenAlertStatusID,
			FiatWithdrawalID:   withdrawalID,
			TradeExecutionID:   sql.NullInt64{},
			Score:              "95.0000",
			DetectedAt:         detectedAt,
			Note:               "status-change-after-alert-procedural",
		}).BuildForTest(t, ctx, tx)
		firstStatusEventID := (&testdb.UserStatusChangeEventBuilder{
			UserID:      userID,
			EventTypeID: masters.FrozenEventTypeID,
			ActorTypeID: masters.SystemActorTypeID,
			ActorID:     "status-after-alert-procedural-batch-1",
			Reason:      "first freeze after alert",
			OccurredAt:  detectedAt.Add(20 * time.Minute),
		}).BuildForTest(t, ctx, tx)
		(&testdb.UserStatusHistoryBuilder{
			UserID:              userID,
			StatusChangeEventID: firstStatusEventID,
			FromStatusID:        sql.NullInt64{Int64: masters.ActiveUserStatusID, Valid: true},
			ToStatusID:          masters.FrozenUserStatusID,
			ChangedAt:           detectedAt.Add(20 * time.Minute),
		}).BuildForTest(t, ctx, tx)
		secondStatusEventID := (&testdb.UserStatusChangeEventBuilder{
			UserID:      userID,
			EventTypeID: masters.FrozenEventTypeID,
			ActorTypeID: masters.SystemActorTypeID,
			ActorID:     "status-after-alert-procedural-batch-2",
			Reason:      "second freeze after alert",
			OccurredAt:  detectedAt.Add(3 * time.Hour),
		}).BuildForTest(t, ctx, tx)
		(&testdb.UserStatusHistoryBuilder{
			UserID:              userID,
			StatusChangeEventID: secondStatusEventID,
			FromStatusID:        sql.NullInt64{Int64: masters.FrozenUserStatusID, Valid: true},
			ToStatusID:          masters.FrozenUserStatusID,
			ChangedAt:           detectedAt.Add(3 * time.Hour),
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/suspicious_transactions/status_change_after_alert_procedural.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				alertEventID    int64
				userIDGot       int64
				memberCodeGot   string
				ruleName        string
				detectedAtGot   time.Time
				statusEventType string
				fromStatus      sql.NullString
				toStatus        sql.NullString
				reason          string
				statusChangedAt time.Time
				delayMinutes    int64
				linkedEventRank int64
				responseBucket  string
			)
			if err := rows.Scan(&alertEventID, &userIDGot, &memberCodeGot, &ruleName, &detectedAtGot, &statusEventType, &fromStatus, &toStatus, &reason, &statusChangedAt, &delayMinutes, &linkedEventRank, &responseBucket); err != nil {
				t.Fatalf("アラート後ステータス変更手続き版の行読み取りに失敗しました: %v", err)
			}
			_ = alertEventID
			_ = detectedAtGot
			_ = statusChangedAt
			if userIDGot == userID {
				found = true
				if memberCodeGot != memberCode || ruleName != "Status Change After Alert Procedural Rule" || statusEventType != "FROZEN" {
					t.Fatalf("アラート後ステータス変更手続き版の属性が期待値と異なります: memberCode=%s rule=%s type=%s", memberCodeGot, ruleName, statusEventType)
				}
				if !fromStatus.Valid || fromStatus.String != "ACTIVE" || !toStatus.Valid || toStatus.String != "FROZEN" {
					t.Fatalf("アラート後ステータス変更手続き版のステータス遷移が期待値と異なります: from=%v to=%v reason=%s", fromStatus, toStatus, reason)
				}
				assertEqualInt64(t, delayMinutes, 20, "手続き版の検知後ステータス変更までの分数")
				assertEqualInt64(t, linkedEventRank, 1, "手続き版の採用順位")
				if responseBucket != "WITHIN_30_MINUTES" {
					t.Fatalf("アラート後ステータス変更手続き版の応答帯が期待値と異なります: actual=%s", responseBucket)
				}
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("アラート後ステータス変更手続き版の走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("アラート後ステータス変更手続き版に挿入した行が見つかりませんでした")
		}
	})
}

func TestLargeUnmatchedCryptoInflowProcedural(t *testing.T) {
	t.Run("高額未対応暗号資産入金手続き版_優先度と理由を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		userBuilder := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID)
		memberCode := userBuilder.MemberCode
		userID := userBuilder.BuildForTest(t, ctx, tx)
		base := time.Date(2031, 4, 4, 9, 0, 0, 0, time.UTC)

		cryptoDepositID := (&testdb.CryptoDepositBuilder{
			UserID:          userID,
			PublicHash:      "large-unmatched-crypto-procedural-deposit",
			CurrencyID:      masters.BTCurrencyID,
			TxHash:          "large-unmatched-crypto-procedural-deposit-tx",
			Amount:          "5.50000000",
			DepositStatusID: masters.CompletedDepositStatusID,
			DetectedAt:      base,
			ConfirmedAt:     sql.NullTime{Time: base.Add(30 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/suspicious_transactions/large_unmatched_crypto_inflow_procedural.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				cryptoDepositIDGot        int64
				userIDGot                 int64
				memberCodeGot             string
				currencyCode              string
				amount                    string
				confirmedAt               time.Time
				matchedSellExecutionCount int64
				matchedWithdrawalCount    int64
				reviewPriority            string
				reviewReason              string
			)
			if err := rows.Scan(&cryptoDepositIDGot, &userIDGot, &memberCodeGot, &currencyCode, &amount, &confirmedAt, &matchedSellExecutionCount, &matchedWithdrawalCount, &reviewPriority, &reviewReason); err != nil {
				t.Fatalf("高額未対応暗号資産入金手続き版の行読み取りに失敗しました: %v", err)
			}
			_ = confirmedAt
			if cryptoDepositIDGot == cryptoDepositID {
				found = true
				if userIDGot != userID || memberCodeGot != memberCode || currencyCode != "BTC" {
					t.Fatalf("高額未対応暗号資産入金手続き版の属性が期待値と異なります: userID=%d memberCode=%s currency=%s", userIDGot, memberCodeGot, currencyCode)
				}
				if amount != "5.500000000000000000" {
					t.Fatalf("高額未対応暗号資産入金手続き版の数量が期待値と異なります: actual=%s", amount)
				}
				assertEqualInt64(t, matchedSellExecutionCount, 0, "手続き版の対応する売却約定件数")
				assertEqualInt64(t, matchedWithdrawalCount, 0, "手続き版の対応する出金件数")
				if reviewPriority != "CRITICAL" || reviewReason != "超大口入金後7日以内の売却・出金なし" {
					t.Fatalf("高額未対応暗号資産入金手続き版の判定結果が期待値と異なります: priority=%s reason=%s", reviewPriority, reviewReason)
				}
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("高額未対応暗号資産入金手続き版の走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("高額未対応暗号資産入金手続き版に挿入した入金行が見つかりませんでした")
		}
	})
}

func TestUserAlertCaseTimelineProcedural(t *testing.T) {
	t.Run("ユーザーイベントタイムライン手続き版_順序番号と段階ラベルを返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		base := time.Date(2031, 4, 5, 9, 0, 0, 0, time.UTC)

		userBuilder := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID)
		memberCode := userBuilder.MemberCode
		userID := userBuilder.BuildForTest(t, ctx, tx)

		withdrawalID := (&testdb.FiatWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "timeline-procedural-withdrawal",
			CurrencyID:         masters.JPYCurrencyID,
			Amount:             "800000",
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        base,
			CompletedAt:        sql.NullTime{Time: base.Add(10 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)
		ruleID := (&testdb.AlertRuleBuilder{
			PublicRuleHash: "9292929292929292929292929292929292929292929292929292929292929292",
			RuleName:       "Timeline Procedural Rule",
			RuleType:       "TIMELINE_PROCEDURAL",
			Severity:       "HIGH",
			ThresholdJSON:  `{"sample":true}`,
		}).BuildForTest(t, ctx, tx)
		alertAt := base.Add(15 * time.Minute)
		alertID := (&testdb.AlertEventLogBuilder{
			UserID:             userID,
			RuleID:             ruleID,
			AlertEventStatusID: masters.OpenAlertStatusID,
			FiatWithdrawalID:   withdrawalID,
			TradeExecutionID:   sql.NullInt64{},
			Score:              "88.8000",
			DetectedAt:         alertAt,
			Note:               "timeline-procedural-alert",
		}).BuildForTest(t, ctx, tx)
		caseID := (&testdb.SuspiciousCaseBuilder{
			UserID:          userID,
			PublicCaseHash:  "9393939393939393939393939393939393939393939393939393939393939393",
			OpenedByTypeID:  masters.SystemActorTypeID,
			OpenedByID:      "integration-test-runner-procedural",
			SourceTypeID:    masters.AutoCaseSourceTypeID,
			AlertEventLogID: sql.NullInt64{Int64: alertID, Valid: true},
			Title:           "timeline procedural case",
			CurrentStatusID: masters.InvestigatingCaseStatusID,
			RiskLevelID:     masters.HighRiskLevelID,
			AssignedTo:      sql.NullString{String: "aml-operator-timeline-procedural", Valid: true},
			OpenedAt:        base.Add(20 * time.Minute),
		}).BuildForTest(t, ctx, tx)
		(&testdb.AccountActionBuilder{
			UserID:           userID,
			SuspiciousCaseID: sql.NullInt64{Int64: caseID, Valid: true},
			ActionTypeID:     masters.FreezeActionTypeID,
			ActorTypeID:      masters.AdminActorTypeID,
			ActorID:          "timeline-procedural-admin",
			ActionReason:     "timeline procedural freeze",
			RequestedAt:      base.Add(25 * time.Minute),
			CompletedAt:      sql.NullTime{Time: base.Add(30 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)
		(&testdb.UserStatusChangeEventBuilder{
			UserID:      userID,
			EventTypeID: masters.FrozenEventTypeID,
			ActorTypeID: masters.SystemActorTypeID,
			ActorID:     "timeline-procedural-batch",
			Reason:      "timeline procedural status change",
			OccurredAt:  base.Add(35 * time.Minute),
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/suspicious_transactions/user_alert_case_timeline_procedural.sql")
		defer rows.Close()

		foundTypes := map[string]bool{}
		sequenceByType := map[string]int64{}
		stageByType := map[string]string{}
		for rows.Next() {
			var (
				userIDGot       int64
				memberCodeGot   string
				eventAt         time.Time
				eventType       string
				eventID         int64
				primaryLabel    string
				secondaryLabel  string
				eventSequenceNo int64
				lifecycleStage  string
			)
			if err := rows.Scan(&userIDGot, &memberCodeGot, &eventAt, &eventType, &eventID, &primaryLabel, &secondaryLabel, &eventSequenceNo, &lifecycleStage); err != nil {
				t.Fatalf("ユーザーイベントタイムライン手続き版の行読み取りに失敗しました: %v", err)
			}
			_ = eventAt
			_ = eventID
			_ = primaryLabel
			_ = secondaryLabel
			if userIDGot == userID {
				if memberCodeGot != memberCode {
					t.Fatalf("ユーザーイベントタイムライン手続き版の会員コードが期待値と異なります: expected=%s actual=%s", memberCode, memberCodeGot)
				}
				foundTypes[eventType] = true
				sequenceByType[eventType] = eventSequenceNo
				stageByType[eventType] = lifecycleStage
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("ユーザーイベントタイムライン手続き版の走査中に失敗しました: %v", err)
		}
		expectedStages := map[string]string{
			"ALERT_DETECTED":      "DETECTION",
			"CASE_OPENED":         "CASE_MANAGEMENT",
			"ACCOUNT_ACTION":      "ACCOUNT_CONTROL",
			"USER_STATUS_CHANGED": "STATUS_CONTROL",
		}
		expectedSequence := map[string]int64{
			"ALERT_DETECTED":      1,
			"CASE_OPENED":         2,
			"ACCOUNT_ACTION":      3,
			"USER_STATUS_CHANGED": 4,
		}
		for eventType, expectedStage := range expectedStages {
			if !foundTypes[eventType] {
				t.Fatalf("ユーザーイベントタイムライン手続き版に期待したイベント種別が見つかりませんでした: eventType=%s", eventType)
			}
			if stageByType[eventType] != expectedStage {
				t.Fatalf("ユーザーイベントタイムライン手続き版の段階ラベルが期待値と異なります: eventType=%s actual=%s", eventType, stageByType[eventType])
			}
			if sequenceByType[eventType] != expectedSequence[eventType] {
				t.Fatalf("ユーザーイベントタイムライン手続き版の順序番号が期待値と異なります: eventType=%s actual=%d", eventType, sequenceByType[eventType])
			}
		}
	})
}

func TestPendingTransactionBacklogSummaryProcedural(t *testing.T) {
	t.Run("滞留中トランザクション手続き版_エスカレーション要否と理由を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		now := currentDBTime(t, ctx, tx)
		userID := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID).BuildForTest(t, ctx, tx)

		(&testdb.FiatDepositBuilder{
			UserID:          userID,
			PublicHash:      "backlog-procedural-pending-fiat-deposit",
			CurrencyID:      masters.JPYCurrencyID,
			Amount:          "123456",
			DepositStatusID: masters.PendingDepositStatusID,
			RequestedAt:     now.Add(-30 * time.Hour),
			CompletedAt:     sql.NullTime{},
		}).BuildForTest(t, ctx, tx)
		(&testdb.CryptoWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "backlog-procedural-pending-crypto-withdrawal",
			CurrencyID:         masters.BTCurrencyID,
			DestinationAddress: "backlog_procedural_pending_btc",
			Amount:             "0.50000000",
			TxHash:             sql.NullString{},
			WithdrawalStatusID: masters.PendingWithdrawalID,
			RequestedAt:        now.Add(-2 * time.Hour),
			CompletedAt:        sql.NullTime{},
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/pending_transaction_backlog_summary_procedural.sql")
		defer rows.Close()

		var foundOver24 bool
		var foundUnder24 bool
		for rows.Next() {
			var (
				operationType     string
				currencyCode      string
				backlogBucket     string
				backlogCount      int64
				oldestStartedAt   time.Time
				maxPendingMinutes int64
				escalationNeeded  int64
				escalationReason  string
			)
			if err := rows.Scan(&operationType, &currencyCode, &backlogBucket, &backlogCount, &oldestStartedAt, &maxPendingMinutes, &escalationNeeded, &escalationReason); err != nil {
				t.Fatalf("滞留中トランザクション手続き版の行読み取りに失敗しました: %v", err)
			}
			_ = oldestStartedAt
			if operationType == "FIAT_DEPOSIT" && currencyCode == "JPY" && backlogBucket == "OVER_24_HOURS" {
				foundOver24 = true
				assertGreaterOrEqualInt64(t, backlogCount, 1, "手続き版の24時間超滞留件数")
				assertGreaterOrEqualInt64(t, maxPendingMinutes, 24*60, "手続き版の24時間超滞留分数")
				if escalationNeeded != 1 || escalationReason != "24時間超の滞留が存在" {
					t.Fatalf("滞留中トランザクション手続き版のエスカレーション判定が期待値と異なります: needed=%d reason=%s", escalationNeeded, escalationReason)
				}
			}
			if operationType == "CRYPTO_WITHDRAWAL" && currencyCode == "BTC" && backlogBucket == "UNDER_24_HOURS" {
				foundUnder24 = true
				assertGreaterOrEqualInt64(t, backlogCount, 1, "手続き版の24時間以内滞留件数")
				assertGreaterOrEqualInt64(t, maxPendingMinutes, 120, "手続き版の24時間以内滞留分数")
				if escalationNeeded != 0 || escalationReason != "通常監視" {
					t.Fatalf("滞留中トランザクション手続き版の通常監視判定が期待値と異なります: needed=%d reason=%s", escalationNeeded, escalationReason)
				}
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("滞留中トランザクション手続き版の走査中に失敗しました: %v", err)
		}
		if !foundOver24 || !foundUnder24 {
			t.Fatalf("滞留中トランザクション手続き版に期待した集計行が見つかりませんでした: over24=%t under24=%t", foundOver24, foundUnder24)
		}
	})
}

func TestMultiHopFundFlowCandidates(t *testing.T) {
	t.Run("多段資金移動候補_入金後の取引経由出金を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		base := time.Date(2031, 4, 6, 9, 0, 0, 0, time.UTC)

		userBuilder := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID)
		memberCode := userBuilder.MemberCode
		userID := userBuilder.BuildForTest(t, ctx, tx)

		(&testdb.FiatDepositBuilder{
			UserID:          userID,
			PublicHash:      "multi-hop-fiat-deposit",
			CurrencyID:      masters.JPYCurrencyID,
			Amount:          "1000000",
			DepositStatusID: masters.CompletedDepositStatusID,
			RequestedAt:     base,
			CompletedAt:     sql.NullTime{Time: base.Add(10 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)

		orderID := (&testdb.TradingOrderBuilder{
			UserID:         userID,
			PublicHash:     "multi-hop-order",
			Side:           "BUY",
			OrderType:      "LIMIT",
			FromCurrencyID: masters.JPYCurrencyID,
			ToCurrencyID:   masters.BTCurrencyID,
			Price:          "1000000",
			Quantity:       "1.00000000",
			OrderStatusID:  masters.FilledOrderStatusID,
			PlacedAt:       base.Add(30 * time.Minute),
		}).BuildForTest(t, ctx, tx)

		tradeExecutionID := (&testdb.TradeExecutionBuilder{
			OrderID:          orderID,
			UserID:           userID,
			PublicHash:       "multi-hop-execution",
			FromCurrencyID:   masters.JPYCurrencyID,
			ToCurrencyID:     masters.BTCurrencyID,
			ExecutedPrice:    "1000000",
			ExecutedQuantity: "1.00000000",
			FromAmount:       "1000000.00000000",
			ToAmount:         "1.00000000",
			FeeCurrencyID:    masters.JPYCurrencyID,
			FeeAmount:        "1000",
			ExecutedAt:       base.Add(40 * time.Minute),
		}).BuildForTest(t, ctx, tx)

		cryptoWithdrawalID := (&testdb.CryptoWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "multi-hop-crypto-withdrawal",
			CurrencyID:         masters.BTCurrencyID,
			DestinationAddress: "multi-hop-destination",
			Amount:             "0.90000000",
			TxHash:             sql.NullString{String: "multi-hop-crypto-withdrawal-tx", Valid: true},
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        base.Add(2 * time.Hour),
			CompletedAt:        sql.NullTime{Time: base.Add(3 * time.Hour), Valid: true},
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/suspicious_transactions/multi_hop_fund_flow_candidates.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				userIDGot                    int64
				memberCodeGot                string
				inflowType                   string
				inflowID                     int64
				inflowCurrencyCode           string
				inflowAmount                 string
				inflowCompletedAt            time.Time
				tradeExecutionIDGot          int64
				intermediateFromCurrencyCode string
				intermediateToCurrencyCode   string
				tradeFromAmount              string
				tradeToAmount                string
				executedAt                   time.Time
				finalOutflowType             string
				finalOutflowID               int64
				finalOutflowCurrencyCode     string
				finalOutflowAmount           string
				outflowCompletedAt           time.Time
				outflowVsTradeRatio          string
			)
			if err := rows.Scan(
				&userIDGot,
				&memberCodeGot,
				&inflowType,
				&inflowID,
				&inflowCurrencyCode,
				&inflowAmount,
				&inflowCompletedAt,
				&tradeExecutionIDGot,
				&intermediateFromCurrencyCode,
				&intermediateToCurrencyCode,
				&tradeFromAmount,
				&tradeToAmount,
				&executedAt,
				&finalOutflowType,
				&finalOutflowID,
				&finalOutflowCurrencyCode,
				&finalOutflowAmount,
				&outflowCompletedAt,
				&outflowVsTradeRatio,
			); err != nil {
				t.Fatalf("多段資金移動候補の行読み取りに失敗しました: %v", err)
			}
			_ = inflowID
			_ = inflowAmount
			_ = inflowCompletedAt
			_ = tradeFromAmount
			_ = tradeToAmount
			_ = executedAt
			_ = outflowCompletedAt
			if userIDGot == userID {
				found = true
				if memberCodeGot != memberCode || inflowType != "FIAT_DEPOSIT" || inflowCurrencyCode != "JPY" {
					t.Fatalf("多段資金移動候補の流入情報が期待値と異なります: member=%s type=%s currency=%s", memberCodeGot, inflowType, inflowCurrencyCode)
				}
				if tradeExecutionIDGot != tradeExecutionID || intermediateFromCurrencyCode != "JPY" || intermediateToCurrencyCode != "BTC" {
					t.Fatalf("多段資金移動候補の約定情報が期待値と異なります: trade=%d from=%s to=%s", tradeExecutionIDGot, intermediateFromCurrencyCode, intermediateToCurrencyCode)
				}
				if finalOutflowType != "CRYPTO_WITHDRAWAL" || finalOutflowID != cryptoWithdrawalID || finalOutflowCurrencyCode != "BTC" || finalOutflowAmount != "0.900000000000000000" {
					t.Fatalf("多段資金移動候補の出金情報が期待値と異なります: type=%s id=%d currency=%s amount=%s", finalOutflowType, finalOutflowID, finalOutflowCurrencyCode, finalOutflowAmount)
				}
				if outflowVsTradeRatio != "0.9000" {
					t.Fatalf("多段資金移動候補の比率が期待値と異なります: actual=%s", outflowVsTradeRatio)
				}
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("多段資金移動候補の走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("多段資金移動候補に挿入した行が見つかりませんでした")
		}
	})
}

func TestAlertCaseActionLeadTime(t *testing.T) {
	t.Run("検知から措置までのリードタイム_ケース化と措置と状態変更の分数を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		base := time.Date(2031, 4, 7, 9, 0, 0, 0, time.UTC)

		userBuilder := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID)
		memberCode := userBuilder.MemberCode
		userID := userBuilder.BuildForTest(t, ctx, tx)

		withdrawalID := (&testdb.FiatWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "lead-time-withdrawal",
			CurrencyID:         masters.JPYCurrencyID,
			Amount:             "900000",
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        base,
			CompletedAt:        sql.NullTime{Time: base.Add(5 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)
		ruleID := (&testdb.AlertRuleBuilder{
			PublicRuleHash: "7070707070707070707070707070707070707070707070707070707070707070",
			RuleName:       "Lead Time Rule",
			RuleType:       "LEAD_TIME",
			Severity:       "HIGH",
			ThresholdJSON:  `{"sample":true}`,
		}).BuildForTest(t, ctx, tx)
		detectedAt := base.Add(10 * time.Minute)
		alertEventID := (&testdb.AlertEventLogBuilder{
			UserID:             userID,
			RuleID:             ruleID,
			AlertEventStatusID: masters.OpenAlertStatusID,
			FiatWithdrawalID:   withdrawalID,
			TradeExecutionID:   sql.NullInt64{},
			Score:              "90.5000",
			DetectedAt:         detectedAt,
			Note:               "lead-time-alert",
		}).BuildForTest(t, ctx, tx)
		caseID := (&testdb.SuspiciousCaseBuilder{
			UserID:          userID,
			PublicCaseHash:  "7171717171717171717171717171717171717171717171717171717171717171",
			OpenedByTypeID:  masters.SystemActorTypeID,
			OpenedByID:      "lead-time-batch",
			SourceTypeID:    masters.AutoCaseSourceTypeID,
			AlertEventLogID: sql.NullInt64{Int64: alertEventID, Valid: true},
			Title:           "lead-time-case",
			CurrentStatusID: masters.InvestigatingCaseStatusID,
			RiskLevelID:     masters.HighRiskLevelID,
			OpenedAt:        detectedAt.Add(15 * time.Minute),
		}).BuildForTest(t, ctx, tx)
		(&testdb.AccountActionBuilder{
			UserID:           userID,
			SuspiciousCaseID: sql.NullInt64{Int64: caseID, Valid: true},
			ActionTypeID:     masters.FreezeActionTypeID,
			ActorTypeID:      masters.AdminActorTypeID,
			ActorID:          "lead-time-admin",
			ActionReason:     "lead-time-freeze",
			RequestedAt:      detectedAt.Add(25 * time.Minute),
			CompletedAt:      sql.NullTime{Time: detectedAt.Add(30 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)
		(&testdb.UserStatusChangeEventBuilder{
			UserID:      userID,
			EventTypeID: masters.FrozenEventTypeID,
			ActorTypeID: masters.SystemActorTypeID,
			ActorID:     "lead-time-status-batch",
			Reason:      "lead-time-status-change",
			OccurredAt:  detectedAt.Add(35 * time.Minute),
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/suspicious_transactions/alert_case_action_lead_time.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				alertEventIDGot          int64
				userIDGot                int64
				memberCodeGot            string
				ruleName                 string
				detectedAtGot            time.Time
				suspiciousCaseID         sql.NullInt64
				caseOpenedAt             sql.NullTime
				firstActionAt            sql.NullTime
				firstStatusChangedAt     sql.NullTime
				caseOpenDelayMinutes     sql.NullInt64
				actionDelayMinutes       sql.NullInt64
				statusChangeDelayMinutes sql.NullInt64
			)
			if err := rows.Scan(&alertEventIDGot, &userIDGot, &memberCodeGot, &ruleName, &detectedAtGot, &suspiciousCaseID, &caseOpenedAt, &firstActionAt, &firstStatusChangedAt, &caseOpenDelayMinutes, &actionDelayMinutes, &statusChangeDelayMinutes); err != nil {
				t.Fatalf("検知から措置までのリードタイムの行読み取りに失敗しました: %v", err)
			}
			_ = detectedAtGot
			_ = caseOpenedAt
			_ = firstActionAt
			_ = firstStatusChangedAt
			if alertEventIDGot == alertEventID {
				found = true
				if userIDGot != userID || memberCodeGot != memberCode || ruleName != "Lead Time Rule" {
					t.Fatalf("検知から措置までのリードタイムの属性が期待値と異なります: user=%d member=%s rule=%s", userIDGot, memberCodeGot, ruleName)
				}
				if !suspiciousCaseID.Valid || suspiciousCaseID.Int64 != caseID {
					t.Fatalf("検知から措置までのリードタイムのケースIDが期待値と異なります: actual=%v", suspiciousCaseID)
				}
				if !caseOpenDelayMinutes.Valid || caseOpenDelayMinutes.Int64 != 15 {
					t.Fatalf("ケース起票遅延分数が期待値と異なります: actual=%v", caseOpenDelayMinutes)
				}
				if !actionDelayMinutes.Valid || actionDelayMinutes.Int64 != 25 {
					t.Fatalf("措置遅延分数が期待値と異なります: actual=%v", actionDelayMinutes)
				}
				if !statusChangeDelayMinutes.Valid || statusChangeDelayMinutes.Int64 != 35 {
					t.Fatalf("状態変更遅延分数が期待値と異なります: actual=%v", statusChangeDelayMinutes)
				}
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("検知から措置までのリードタイムの走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("検知から措置までのリードタイムに挿入した行が見つかりませんでした")
		}
	})
}

func TestSameDestinationClusterSummary(t *testing.T) {
	t.Run("同一送金先クラスタ集計_複数ユーザーの集中出金を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		now := currentDBTime(t, ctx, tx)

		user1 := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID)
		memberCode1 := user1.MemberCode
		userID1 := user1.BuildForTest(t, ctx, tx)
		user2 := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID)
		memberCode2 := user2.MemberCode
		userID2 := user2.BuildForTest(t, ctx, tx)

		address := "same-destination-cluster-address"
		(&testdb.CryptoWithdrawalBuilder{
			UserID:             userID1,
			PublicHash:         "same-destination-cluster-withdrawal-1",
			CurrencyID:         masters.BTCurrencyID,
			DestinationAddress: address,
			Amount:             "0.70000000",
			TxHash:             sql.NullString{String: "same-destination-cluster-tx-1", Valid: true},
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        now.Add(-3 * time.Hour),
			CompletedAt:        sql.NullTime{Time: now.Add(-2 * time.Hour), Valid: true},
		}).BuildForTest(t, ctx, tx)
		(&testdb.CryptoWithdrawalBuilder{
			UserID:             userID2,
			PublicHash:         "same-destination-cluster-withdrawal-2",
			CurrencyID:         masters.BTCurrencyID,
			DestinationAddress: address,
			Amount:             "0.80000000",
			TxHash:             sql.NullString{String: "same-destination-cluster-tx-2", Valid: true},
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        now.Add(-90 * time.Minute),
			CompletedAt:        sql.NullTime{Time: now.Add(-60 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/suspicious_transactions/same_destination_cluster_summary.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				destinationAddress string
				withdrawalCount    int64
				userCount          int64
				totalAmount        string
				clusterStartAt     time.Time
				clusterEndAt       time.Time
				memberCodes        string
			)
			if err := rows.Scan(&destinationAddress, &withdrawalCount, &userCount, &totalAmount, &clusterStartAt, &clusterEndAt, &memberCodes); err != nil {
				t.Fatalf("同一送金先クラスタ集計の行読み取りに失敗しました: %v", err)
			}
			_ = clusterStartAt
			_ = clusterEndAt
			if destinationAddress == address {
				found = true
				assertEqualInt64(t, withdrawalCount, 2, "同一送金先クラスタの出金件数")
				assertEqualInt64(t, userCount, 2, "同一送金先クラスタのユーザー数")
				if totalAmount != "1.500000000000000000" {
					t.Fatalf("同一送金先クラスタの総出金量が期待値と異なります: actual=%s", totalAmount)
				}
				if !(strings.Contains(memberCodes, memberCode1) && strings.Contains(memberCodes, memberCode2)) {
					t.Fatalf("同一送金先クラスタの会員コード一覧が期待値と異なります: actual=%s", memberCodes)
				}
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("同一送金先クラスタ集計の走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("同一送金先クラスタ集計に挿入した行が見つかりませんでした")
		}
	})
}

func TestSuspiciousSplitWithdrawalCandidates(t *testing.T) {
	t.Run("疑わしい取引_同一送金先への分割出金候補を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		now := currentDBTime(t, ctx, tx)

		userBuilder := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID)
		memberCode := userBuilder.MemberCode
		userID := userBuilder.BuildForTest(t, ctx, tx)

		address := "split-withdrawal-candidate-address"
		for i, amount := range []string{"0.40000000", "0.35000000", "0.25000000"} {
			(&testdb.CryptoWithdrawalBuilder{
				UserID:             userID,
				PublicHash:         fmt.Sprintf("split-withdrawal-%d", i+1),
				CurrencyID:         masters.BTCurrencyID,
				DestinationAddress: address,
				Amount:             amount,
				TxHash:             sql.NullString{String: fmt.Sprintf("split-withdrawal-tx-%d", i+1), Valid: true},
				WithdrawalStatusID: masters.CompletedWithdrawalID,
				RequestedAt:        now.Add(time.Duration(-4+i) * time.Hour),
				CompletedAt:        sql.NullTime{Time: now.Add(time.Duration(-3+i) * time.Hour), Valid: true},
			}).BuildForTest(t, ctx, tx)
		}

		rows := queryRows(t, ctx, tx, "examples/suspicious_transactions/suspicious_split_withdrawal_candidates.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				userIDGot          int64
				memberCodeGot      string
				currencyCode       string
				destinationAddress string
				withdrawalCount    int64
				totalAmount        string
				firstWithdrawalAt  time.Time
				lastWithdrawalAt   time.Time
				spreadMinutes      int64
			)
			if err := rows.Scan(&userIDGot, &memberCodeGot, &currencyCode, &destinationAddress, &withdrawalCount, &totalAmount, &firstWithdrawalAt, &lastWithdrawalAt, &spreadMinutes); err != nil {
				t.Fatalf("分割出金候補SQLの行読み取りに失敗しました: %v", err)
			}
			_ = firstWithdrawalAt
			_ = lastWithdrawalAt
			if userIDGot == userID && destinationAddress == address {
				found = true
				if memberCodeGot != memberCode || currencyCode != "BTC" {
					t.Fatalf("分割出金候補SQLの属性が期待値と異なります: member=%s currency=%s", memberCodeGot, currencyCode)
				}
				assertEqualInt64(t, withdrawalCount, 3, "分割出金候補の出金件数")
				if totalAmount != "1.000000000000000000" {
					t.Fatalf("分割出金候補の総出金量が期待値と異なります: actual=%s", totalAmount)
				}
				assertEqualInt64(t, spreadMinutes, 120, "分割出金候補の時間幅")
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("分割出金候補SQLの走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("分割出金候補SQLに挿入した行が見つかりませんでした")
		}
	})
}

func TestCaseReopenAndRealertSummary(t *testing.T) {
	t.Run("再オープン再検知ケース集計_再開と再アラート件数を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		base := time.Date(2031, 4, 8, 9, 0, 0, 0, time.UTC)

		userBuilder := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID)
		memberCode := userBuilder.MemberCode
		userID := userBuilder.BuildForTest(t, ctx, tx)

		initialWithdrawalID := (&testdb.FiatWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "case-reopen-initial-withdrawal",
			CurrencyID:         masters.JPYCurrencyID,
			Amount:             "600000",
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        base,
			CompletedAt:        sql.NullTime{Time: base.Add(10 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)
		initialRuleID := (&testdb.AlertRuleBuilder{
			PublicRuleHash: "6060606060606060606060606060606060606060606060606060606060606060",
			RuleName:       "Case Reopen Initial Rule",
			RuleType:       "CASE_REOPEN_INITIAL",
			Severity:       "HIGH",
			ThresholdJSON:  `{"sample":true}`,
		}).BuildForTest(t, ctx, tx)
		initialAlertID := (&testdb.AlertEventLogBuilder{
			UserID:             userID,
			RuleID:             initialRuleID,
			AlertEventStatusID: masters.OpenAlertStatusID,
			FiatWithdrawalID:   initialWithdrawalID,
			TradeExecutionID:   sql.NullInt64{},
			Score:              "80.0000",
			DetectedAt:         base.Add(20 * time.Minute),
			Note:               "case-reopen-initial-alert",
		}).BuildForTest(t, ctx, tx)

		caseID := (&testdb.SuspiciousCaseBuilder{
			UserID:          userID,
			PublicCaseHash:  "6161616161616161616161616161616161616161616161616161616161616161",
			OpenedByTypeID:  masters.SystemActorTypeID,
			OpenedByID:      "case-reopen-batch",
			SourceTypeID:    masters.AutoCaseSourceTypeID,
			AlertEventLogID: sql.NullInt64{Int64: initialAlertID, Valid: true},
			Title:           "case-reopen-case",
			CurrentStatusID: masters.InvestigatingCaseStatusID,
			RiskLevelID:     masters.HighRiskLevelID,
			OpenedAt:        base.Add(25 * time.Minute),
			ClosedAt:        sql.NullTime{Time: base.Add(2 * time.Hour), Valid: true},
			ClosedReason:    sql.NullString{String: "initial close", Valid: true},
			Disposition:     sql.NullString{String: "FALSE_POSITIVE", Valid: true},
		}).BuildForTest(t, ctx, tx)

		(&testdb.CaseStatusHistoryBuilder{
			CaseID:       caseID,
			FromStatusID: sql.NullInt64{Int64: masters.InvestigatingCaseStatusID, Valid: true},
			ToStatusID:   masters.ClosedCaseStatusID,
			ActorTypeID:  masters.AdminActorTypeID,
			ActorID:      "case-reopen-admin-close",
			Reason:       "close case",
			ChangedAt:    base.Add(2 * time.Hour),
		}).BuildForTest(t, ctx, tx)
		(&testdb.CaseStatusHistoryBuilder{
			CaseID:       caseID,
			FromStatusID: sql.NullInt64{Int64: masters.ClosedCaseStatusID, Valid: true},
			ToStatusID:   masters.InvestigatingCaseStatusID,
			ActorTypeID:  masters.AdminActorTypeID,
			ActorID:      "case-reopen-admin-reopen",
			Reason:       "reopen case",
			ChangedAt:    base.Add(3 * time.Hour),
		}).BuildForTest(t, ctx, tx)

		realertWithdrawalID := (&testdb.FiatWithdrawalBuilder{
			UserID:             userID,
			PublicHash:         "case-reopen-realert-withdrawal",
			CurrencyID:         masters.JPYCurrencyID,
			Amount:             "650000",
			WithdrawalStatusID: masters.CompletedWithdrawalID,
			RequestedAt:        base.Add(4 * time.Hour),
			CompletedAt:        sql.NullTime{Time: base.Add(4*time.Hour + 10*time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)
		realertRuleID := (&testdb.AlertRuleBuilder{
			PublicRuleHash: "6262626262626262626262626262626262626262626262626262626262626262",
			RuleName:       "Case Reopen Realert Rule",
			RuleType:       "CASE_REOPEN_REALERT",
			Severity:       "CRITICAL",
			ThresholdJSON:  `{"sample":true}`,
		}).BuildForTest(t, ctx, tx)
		(&testdb.AlertEventLogBuilder{
			UserID:             userID,
			RuleID:             realertRuleID,
			AlertEventStatusID: masters.OpenAlertStatusID,
			FiatWithdrawalID:   realertWithdrawalID,
			TradeExecutionID:   sql.NullInt64{},
			Score:              "92.0000",
			DetectedAt:         base.Add(4*time.Hour + 20*time.Minute),
			Note:               "case-reopen-realert",
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/suspicious_transactions/case_reopen_and_realert_summary.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				suspiciousCaseID int64
				userIDGot        int64
				memberCodeGot    string
				title            string
				currentStatus    string
				openedAt         time.Time
				closedAt         sql.NullTime
				reopenCount      int64
				reAlertCount     int64
			)
			if err := rows.Scan(&suspiciousCaseID, &userIDGot, &memberCodeGot, &title, &currentStatus, &openedAt, &closedAt, &reopenCount, &reAlertCount); err != nil {
				t.Fatalf("再オープン再検知ケース集計の行読み取りに失敗しました: %v", err)
			}
			_ = openedAt
			_ = closedAt
			if suspiciousCaseID == caseID {
				found = true
				if userIDGot != userID || memberCodeGot != memberCode || title != "case-reopen-case" || currentStatus != "INVESTIGATING" {
					t.Fatalf("再オープン再検知ケース集計の属性が期待値と異なります: user=%d member=%s title=%s status=%s", userIDGot, memberCodeGot, title, currentStatus)
				}
				assertEqualInt64(t, reopenCount, 1, "ケース再オープン件数")
				assertEqualInt64(t, reAlertCount, 1, "ケース再検知件数")
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("再オープン再検知ケース集計の走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("再オープン再検知ケース集計に挿入したケース行が見つかりませんでした")
		}
	})
}

func TestBalanceGapRootCauseBreakdown(t *testing.T) {
	t.Run("残高差分主因内訳_最大影響要因と件数を返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		base := time.Date(2031, 4, 9, 9, 0, 0, 0, time.UTC)

		userBuilder := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID)
		memberCode := userBuilder.MemberCode
		userID := userBuilder.BuildForTest(t, ctx, tx)

		(&testdb.FiatDepositBuilder{
			UserID:          userID,
			PublicHash:      "balance-breakdown-fiat-deposit",
			CurrencyID:      masters.JPYCurrencyID,
			Amount:          "1200000",
			DepositStatusID: masters.CompletedDepositStatusID,
			RequestedAt:     base,
			CompletedAt:     sql.NullTime{Time: base.Add(5 * time.Minute), Valid: true},
		}).BuildForTest(t, ctx, tx)

		orderID := (&testdb.TradingOrderBuilder{
			UserID:         userID,
			PublicHash:     "balance-breakdown-order",
			Side:           "BUY",
			OrderType:      "LIMIT",
			FromCurrencyID: masters.JPYCurrencyID,
			ToCurrencyID:   masters.BTCurrencyID,
			Price:          "500000",
			Quantity:       "1.00000000",
			OrderStatusID:  masters.FilledOrderStatusID,
			PlacedAt:       base.Add(10 * time.Minute),
		}).BuildForTest(t, ctx, tx)
		(&testdb.TradeExecutionBuilder{
			OrderID:          orderID,
			UserID:           userID,
			PublicHash:       "balance-breakdown-execution",
			FromCurrencyID:   masters.JPYCurrencyID,
			ToCurrencyID:     masters.BTCurrencyID,
			ExecutedPrice:    "500000",
			ExecutedQuantity: "1.00000000",
			FromAmount:       "500000.00000000",
			ToAmount:         "1.00000000",
			FeeCurrencyID:    masters.JPYCurrencyID,
			FeeAmount:        "500",
			ExecutedAt:       base.Add(15 * time.Minute),
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/balance_gap_root_cause_breakdown.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				userIDGot               int64
				memberCodeGot           string
				currencyCode            string
				totalEventCount         int64
				externalNetAmount       string
				tradeNetAmount          string
				feeAmount               string
				theoreticalBalanceDelta string
				maxExternalImpact       string
				maxTradeImpact          string
				maxFeeImpact            string
				dominantCause           string
			)
			if err := rows.Scan(&userIDGot, &memberCodeGot, &currencyCode, &totalEventCount, &externalNetAmount, &tradeNetAmount, &feeAmount, &theoreticalBalanceDelta, &maxExternalImpact, &maxTradeImpact, &maxFeeImpact, &dominantCause); err != nil {
				t.Fatalf("残高差分主因内訳の行読み取りに失敗しました: %v", err)
			}
			if userIDGot == userID && currencyCode == "JPY" {
				found = true
				if memberCodeGot != memberCode {
					t.Fatalf("残高差分主因内訳の会員コードが期待値と異なります: expected=%s actual=%s", memberCode, memberCodeGot)
				}
				assertEqualInt64(t, totalEventCount, 3, "残高差分主因内訳のイベント件数")
				if externalNetAmount != "1200000.000000000000000000" || tradeNetAmount != "-500000.000000000000000000" || feeAmount != "500.000000000000000000" || theoreticalBalanceDelta != "699500.000000000000000000" {
					t.Fatalf("残高差分主因内訳の差分値が期待値と異なります: external=%s trade=%s fee=%s delta=%s", externalNetAmount, tradeNetAmount, feeAmount, theoreticalBalanceDelta)
				}
				if maxExternalImpact != "1200000.000000000000000000" || maxTradeImpact != "500000.000000000000000000" || maxFeeImpact != "500.000000000000000000" || dominantCause != "EXTERNAL_FLOW" {
					t.Fatalf("残高差分主因内訳の主因判定が期待値と異なります: external=%s trade=%s fee=%s cause=%s", maxExternalImpact, maxTradeImpact, maxFeeImpact, dominantCause)
				}
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("残高差分主因内訳の走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("残高差分主因内訳に挿入したJPY行が見つかりませんでした")
		}
	})
}

func TestStuckCaseQueue(t *testing.T) {
	t.Run("滞留ケースキュー_長時間動いていないケースを返す", func(t *testing.T) {
		ctx, tx, masters := setupTest(t)
		now := currentDBTime(t, ctx, tx)

		userBuilder := testdb.NewUserBuilder().WithStatusID(masters.ActiveUserStatusID)
		memberCode := userBuilder.MemberCode
		userID := userBuilder.BuildForTest(t, ctx, tx)

		caseID := (&testdb.SuspiciousCaseBuilder{
			UserID:          userID,
			PublicCaseHash:  "6363636363636363636363636363636363636363636363636363636363636363",
			OpenedByTypeID:  masters.SystemActorTypeID,
			OpenedByID:      "stuck-case-batch",
			SourceTypeID:    masters.AutoCaseSourceTypeID,
			Title:           "stuck-case",
			CurrentStatusID: masters.ActionRequiredCaseStatusID,
			RiskLevelID:     masters.CriticalRiskLevelID,
			AssignedTo:      sql.NullString{String: "aml-stuck-owner", Valid: true},
			OpenedAt:        now.Add(-96 * time.Hour),
		}).BuildForTest(t, ctx, tx)

		(&testdb.CaseStatusHistoryBuilder{
			CaseID:       caseID,
			FromStatusID: sql.NullInt64{Int64: masters.OpenCaseStatusID, Valid: true},
			ToStatusID:   masters.ActionRequiredCaseStatusID,
			ActorTypeID:  masters.AdminActorTypeID,
			ActorID:      "stuck-case-admin",
			Reason:       "need action",
			ChangedAt:    now.Add(-72 * time.Hour),
		}).BuildForTest(t, ctx, tx)

		rows := queryRows(t, ctx, tx, "examples/suspicious_transactions/stuck_case_queue.sql")
		defer rows.Close()

		var found bool
		for rows.Next() {
			var (
				suspiciousCaseID  int64
				userIDGot         int64
				memberCodeGot     string
				title             string
				currentCaseStatus string
				riskLevel         string
				assignedTo        sql.NullString
				lastActivityAt    time.Time
				pendingCaseMins   int64
			)
			if err := rows.Scan(&suspiciousCaseID, &userIDGot, &memberCodeGot, &title, &currentCaseStatus, &riskLevel, &assignedTo, &lastActivityAt, &pendingCaseMins); err != nil {
				t.Fatalf("滞留ケースキューの行読み取りに失敗しました: %v", err)
			}
			_ = lastActivityAt
			if suspiciousCaseID == caseID {
				found = true
				if userIDGot != userID || memberCodeGot != memberCode || title != "stuck-case" || currentCaseStatus != "ACTION_REQUIRED" || riskLevel != "CRITICAL" {
					t.Fatalf("滞留ケースキューの属性が期待値と異なります: user=%d member=%s title=%s status=%s risk=%s", userIDGot, memberCodeGot, title, currentCaseStatus, riskLevel)
				}
				if !assignedTo.Valid || assignedTo.String != "aml-stuck-owner" {
					t.Fatalf("滞留ケースキューの担当者が期待値と異なります: actual=%v", assignedTo)
				}
				assertGreaterOrEqualInt64(t, pendingCaseMins, 48*60, "滞留ケースキューの滞留分数")
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("滞留ケースキューの走査中に失敗しました: %v", err)
		}
		if !found {
			t.Fatal("滞留ケースキューに挿入したケース行が見つかりませんでした")
		}
	})
}

func setupTest(t *testing.T) (context.Context, *sql.Tx, testdb.MasterData) {
	t.Helper()

	db := testdb.Open(t)
	tx := testdb.BeginRollbackTx(t, db)
	ctx := context.Background()
	masters := testdb.LoadMasterDataForTest(t, ctx, tx)

	return ctx, tx, masters
}

func currentDBTime(t *testing.T, ctx context.Context, tx *sql.Tx) time.Time {
	t.Helper()

	var current time.Time
	if err := tx.QueryRowContext(ctx, "SELECT CURRENT_TIMESTAMP").Scan(&current); err != nil {
		t.Fatalf("データベース現在時刻の取得に失敗しました: %v", err)
	}

	return current
}

func insertFailureRateSeedData(t *testing.T, ctx context.Context, tx *sql.Tx, masters testdb.MasterData, userID int64) {
	fiatDepositDate := time.Date(2031, 1, 11, 9, 0, 0, 0, time.UTC)
	fiatWithdrawalDate := time.Date(2031, 1, 12, 9, 0, 0, 0, time.UTC)
	cryptoDepositDate := time.Date(2031, 1, 13, 9, 0, 0, 0, time.UTC)
	cryptoWithdrawalDate := time.Date(2031, 1, 14, 9, 0, 0, 0, time.UTC)

	(&testdb.FiatDepositBuilder{
		UserID:          userID,
		PublicHash:      "failure-rate-fiat-deposit-completed",
		CurrencyID:      masters.JPYCurrencyID,
		Amount:          "1000",
		DepositStatusID: masters.CompletedDepositStatusID,
		RequestedAt:     fiatDepositDate,
		CompletedAt:     sql.NullTime{Time: fiatDepositDate.Add(10 * time.Minute), Valid: true},
	}).BuildForTest(t, ctx, tx)
	(&testdb.FiatDepositBuilder{
		UserID:          userID,
		PublicHash:      "failure-rate-fiat-deposit-failed",
		CurrencyID:      masters.JPYCurrencyID,
		Amount:          "1000",
		DepositStatusID: masters.FailedDepositStatusID,
		RequestedAt:     fiatDepositDate.Add(time.Hour),
		FailedAt:        sql.NullTime{Time: fiatDepositDate.Add(70 * time.Minute), Valid: true},
	}).BuildForTest(t, ctx, tx)

	(&testdb.FiatWithdrawalBuilder{
		UserID:             userID,
		PublicHash:         "failure-rate-fiat-withdrawal-completed",
		CurrencyID:         masters.JPYCurrencyID,
		Amount:             "1000",
		WithdrawalStatusID: masters.CompletedWithdrawalID,
		RequestedAt:        fiatWithdrawalDate,
		CompletedAt:        sql.NullTime{Time: fiatWithdrawalDate.Add(10 * time.Minute), Valid: true},
	}).BuildForTest(t, ctx, tx)
	(&testdb.FiatWithdrawalBuilder{
		UserID:             userID,
		PublicHash:         "failure-rate-fiat-withdrawal-failed",
		CurrencyID:         masters.JPYCurrencyID,
		Amount:             "1000",
		WithdrawalStatusID: masters.FailedWithdrawalID,
		RequestedAt:        fiatWithdrawalDate.Add(time.Hour),
		FailedAt:           sql.NullTime{Time: fiatWithdrawalDate.Add(80 * time.Minute), Valid: true},
	}).BuildForTest(t, ctx, tx)

	(&testdb.CryptoDepositBuilder{
		UserID:          userID,
		PublicHash:      "failure-rate-crypto-deposit-completed",
		CurrencyID:      masters.BTCurrencyID,
		TxHash:          "failure-rate-crypto-deposit-completed-tx",
		Amount:          "0.50000000",
		DepositStatusID: masters.CompletedDepositStatusID,
		DetectedAt:      cryptoDepositDate,
		ConfirmedAt:     sql.NullTime{Time: cryptoDepositDate.Add(10 * time.Minute), Valid: true},
	}).BuildForTest(t, ctx, tx)
	(&testdb.CryptoDepositBuilder{
		UserID:          userID,
		PublicHash:      "failure-rate-crypto-deposit-failed",
		CurrencyID:      masters.BTCurrencyID,
		TxHash:          "failure-rate-crypto-deposit-failed-tx",
		Amount:          "0.50000000",
		DepositStatusID: masters.FailedDepositStatusID,
		DetectedAt:      cryptoDepositDate.Add(time.Hour),
		FailedAt:        sql.NullTime{Time: cryptoDepositDate.Add(70 * time.Minute), Valid: true},
	}).BuildForTest(t, ctx, tx)

	(&testdb.CryptoWithdrawalBuilder{
		UserID:             userID,
		PublicHash:         "failure-rate-crypto-withdrawal-completed",
		CurrencyID:         masters.BTCurrencyID,
		DestinationAddress: "failure-rate-completed-address",
		Amount:             "0.30000000",
		TxHash:             sql.NullString{String: "failure-rate-crypto-withdrawal-completed-tx", Valid: true},
		WithdrawalStatusID: masters.CompletedWithdrawalID,
		RequestedAt:        cryptoWithdrawalDate,
		CompletedAt:        sql.NullTime{Time: cryptoWithdrawalDate.Add(20 * time.Minute), Valid: true},
	}).BuildForTest(t, ctx, tx)
	(&testdb.CryptoWithdrawalBuilder{
		UserID:             userID,
		PublicHash:         "failure-rate-crypto-withdrawal-failed",
		CurrencyID:         masters.BTCurrencyID,
		DestinationAddress: "failure-rate-failed-address",
		Amount:             "0.30000000",
		WithdrawalStatusID: masters.FailedWithdrawalID,
		RequestedAt:        cryptoWithdrawalDate.Add(time.Hour),
		FailedAt:           sql.NullTime{Time: cryptoWithdrawalDate.Add(80 * time.Minute), Valid: true},
	}).BuildForTest(t, ctx, tx)
}

func queryRows(t *testing.T, ctx context.Context, tx *sql.Tx, relativePath string) *sql.Rows {
	t.Helper()

	path := filepath.Join("..", relativePath)
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("SQLファイルの読み込みに失敗しました path=%s err=%v", path, err)
	}

	setupQuery, resultQuery := testdb.SplitProceduralSQL(string(content))
	if setupQuery != "" {
		if _, err := tx.ExecContext(ctx, setupQuery); err != nil {
			t.Fatalf("SQLセットアップの実行に失敗しました path=%s err=%v", path, err)
		}
	}

	rows, err := tx.QueryContext(ctx, resultQuery)
	if err != nil {
		t.Fatalf("SQLの実行に失敗しました path=%s err=%v", path, err)
	}

	return rows
}

func assertEqualInt64(t *testing.T, actual int64, expected int64, label string) {
	t.Helper()

	if actual != expected {
		t.Fatalf("%s が期待値と異なります: expected=%d actual=%d", label, expected, actual)
	}
}

func assertGreaterOrEqualInt64(t *testing.T, actual int64, expected int64, label string) {
	t.Helper()

	if actual < expected {
		t.Fatalf("%s が期待値未満です: expected_at_least=%d actual=%d", label, expected, actual)
	}
}
