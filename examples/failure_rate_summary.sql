USE exchange_domain;

/*
  目的:
  - 取引・入出金の「失敗率」を日次で確認し、運用上の異常兆候を早期に把握する。
  - どの業務(法定入金/法定出金/暗号資産入金/暗号資産出金)で失敗が多いか比較する。

  ロジック:
  - 各テーブルを「業務種別」「日付」「総件数」「失敗件数」に正規化して UNION ALL。
  - 外側で 失敗率(%) = failed_count / total_count * 100 を算出。

  出力の読み方:
  - failure_rate_pct が高い行を優先的に調査する。
  - 同日の他業務と比較すると、障害が業務横断か局所的かを切り分けやすい。
*/
SELECT
  -- 比較対象の業務種別。
  operation_type,
  -- 日次の観測日。
  activity_date,
  -- その日の総処理件数。
  total_count,
  -- その日の失敗件数。
  failed_count,
  -- ゼロ除算を避けつつ失敗率を百分率で表示。
  ROUND((failed_count / NULLIF(total_count, 0)) * 100, 2) AS failure_rate_pct
FROM (
  SELECT
    -- 法定入金の集計ブロック。
    'FIAT_DEPOSIT' AS operation_type,
    DATE(requested_at) AS activity_date,
    COUNT(*) AS total_count,
    -- ステータスが FAILED の行だけ失敗件数に加算。
    SUM(CASE WHEN ds.value = 'FAILED' THEN 1 ELSE 0 END) AS failed_count
  FROM fiat_deposits
  -- ステータスIDを業務的に解釈するためマスタ結合。
  INNER JOIN deposit_statuses ds ON ds.id = fiat_deposits.deposit_status_id
  GROUP BY DATE(requested_at)

  UNION ALL

  SELECT
    -- 法定出金の集計ブロック。
    'FIAT_WITHDRAWAL' AS operation_type,
    DATE(requested_at) AS activity_date,
    COUNT(*) AS total_count,
    SUM(CASE WHEN ws.value = 'FAILED' THEN 1 ELSE 0 END) AS failed_count
  FROM fiat_withdrawals
  INNER JOIN withdrawal_statuses ws ON ws.id = fiat_withdrawals.withdrawal_status_id
  GROUP BY DATE(requested_at)

  UNION ALL

  SELECT
    -- 暗号資産入金の集計ブロック。
    'CRYPTO_DEPOSIT' AS operation_type,
    DATE(detected_at) AS activity_date,
    COUNT(*) AS total_count,
    SUM(CASE WHEN ds.value = 'FAILED' THEN 1 ELSE 0 END) AS failed_count
  FROM crypto_deposits
  INNER JOIN deposit_statuses ds ON ds.id = crypto_deposits.deposit_status_id
  GROUP BY DATE(detected_at)

  UNION ALL

  SELECT
    -- 暗号資産出金の集計ブロック。
    'CRYPTO_WITHDRAWAL' AS operation_type,
    DATE(requested_at) AS activity_date,
    COUNT(*) AS total_count,
    SUM(CASE WHEN ws.value = 'FAILED' THEN 1 ELSE 0 END) AS failed_count
  FROM crypto_withdrawals
  INNER JOIN withdrawal_statuses ws ON ws.id = crypto_withdrawals.withdrawal_status_id
  GROUP BY DATE(requested_at)
) t
ORDER BY
  -- まず日付で束ねる。
  activity_date,
  -- 同日内では失敗率が高い順に見る。
  failure_rate_pct DESC,
  -- 同率時の表示順を安定化。
  operation_type;
