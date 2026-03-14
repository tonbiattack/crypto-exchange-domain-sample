package testdb

import (
	"context"
	"database/sql"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	defaultDSN = "app:app@tcp(127.0.0.1:33306)/exchange_domain?parseTime=true&multiStatements=true"
)

var useStatementPattern = regexp.MustCompile(`(?im)^\s*USE\s+exchange_domain\s*;\s*`)

const proceduralResultMarker = "-- RESULT_QUERY"

func Open(t *testing.T) *sql.DB {
	t.Helper()

	dsn := os.Getenv("TEST_DB_DSN")
	if dsn == "" {
		dsn = defaultDSN
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("データベース接続の初期化に失敗しました: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("データベースへの疎通確認に失敗しました: %v", err)
	}

	return db
}

func BeginRollbackTx(t *testing.T, db *sql.DB) *sql.Tx {
	t.Helper()

	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		t.Fatalf("ロールバック前提のトランザクション開始に失敗しました: %v", err)
	}

	t.Cleanup(func() {
		_ = tx.Rollback()
	})

	return tx
}

func NormalizeSQL(raw string) string {
	trimmed := strings.TrimSpace(raw)
	trimmed = useStatementPattern.ReplaceAllString(trimmed, "")
	return strings.TrimSpace(trimmed)
}

func SplitProceduralSQL(raw string) (setup string, resultQuery string) {
	normalized := NormalizeSQL(raw)
	parts := strings.SplitN(normalized, proceduralResultMarker, 2)
	if len(parts) == 1 {
		return "", normalized
	}

	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
}
