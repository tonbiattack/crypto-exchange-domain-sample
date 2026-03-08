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
  operation_type,
  activity_date,
  total_count,
  failed_count,
  ROUND((failed_count / NULLIF(total_count, 0)) * 100, 2) AS failure_rate_pct
FROM (
  SELECT
    'FIAT_DEPOSIT' AS operation_type,
    DATE(requested_at) AS activity_date,
    COUNT(*) AS total_count,
    SUM(CASE WHEN ds.value = 'FAILED' THEN 1 ELSE 0 END) AS failed_count
  FROM fiat_deposits
  INNER JOIN deposit_statuses ds ON ds.id = fiat_deposits.deposit_status_id
  GROUP BY DATE(requested_at)

  UNION ALL

  SELECT
    'FIAT_WITHDRAWAL' AS operation_type,
    DATE(requested_at) AS activity_date,
    COUNT(*) AS total_count,
    SUM(CASE WHEN ws.value = 'FAILED' THEN 1 ELSE 0 END) AS failed_count
  FROM fiat_withdrawals
  INNER JOIN withdrawal_statuses ws ON ws.id = fiat_withdrawals.withdrawal_status_id
  GROUP BY DATE(requested_at)

  UNION ALL

  SELECT
    'CRYPTO_DEPOSIT' AS operation_type,
    DATE(detected_at) AS activity_date,
    COUNT(*) AS total_count,
    SUM(CASE WHEN ds.value = 'FAILED' THEN 1 ELSE 0 END) AS failed_count
  FROM crypto_deposits
  INNER JOIN deposit_statuses ds ON ds.id = crypto_deposits.deposit_status_id
  GROUP BY DATE(detected_at)

  UNION ALL

  SELECT
    'CRYPTO_WITHDRAWAL' AS operation_type,
    DATE(requested_at) AS activity_date,
    COUNT(*) AS total_count,
    SUM(CASE WHEN ws.value = 'FAILED' THEN 1 ELSE 0 END) AS failed_count
  FROM crypto_withdrawals
  INNER JOIN withdrawal_statuses ws ON ws.id = crypto_withdrawals.withdrawal_status_id
  GROUP BY DATE(requested_at)
) t
ORDER BY
  activity_date,
  failure_rate_pct DESC,
  operation_type;
