USE exchange_domain;

/*
  目的:
  - PENDING 入出金の滞留状況を、業務種別・通貨別・滞留帯別に集計する。
  - 一覧より先に全体傾向を掴みたい運用ダッシュボード向け。

  見方:
  - pending_transactions で各業務テーブルの処理中イベントを
    (operation_type, currency_id, started_at) に正規化してから集計する。
  - backlog_bucket は滞留帯で、
    UNDER_1_HOUR / UNDER_24_HOURS / OVER_24_HOURS に分類する。
  - backlog_count が多く、max_pending_minutes が大きい帯ほど運用優先度が高い。
*/
WITH pending_transactions AS (
  -- 法定入金の処理中案件。
  SELECT 'FIAT_DEPOSIT' AS operation_type, fd.currency_id, fd.requested_at AS started_at
  FROM fiat_deposits fd
  INNER JOIN deposit_statuses ds ON ds.id = fd.deposit_status_id
  WHERE ds.value = 'PENDING'

  UNION ALL

  -- 法定出金の処理中案件。
  SELECT 'FIAT_WITHDRAWAL' AS operation_type, fw.currency_id, fw.requested_at AS started_at
  FROM fiat_withdrawals fw
  INNER JOIN withdrawal_statuses ws ON ws.id = fw.withdrawal_status_id
  WHERE ws.value = 'PENDING'

  UNION ALL

  -- 暗号資産入金は detected_at を滞留開始時刻として扱う。
  SELECT 'CRYPTO_DEPOSIT' AS operation_type, cd.currency_id, cd.detected_at AS started_at
  FROM crypto_deposits cd
  INNER JOIN deposit_statuses ds ON ds.id = cd.deposit_status_id
  WHERE ds.value = 'PENDING'

  UNION ALL

  -- 暗号資産出金の処理中案件。
  SELECT 'CRYPTO_WITHDRAWAL' AS operation_type, cw.currency_id, cw.requested_at AS started_at
  FROM crypto_withdrawals cw
  INNER JOIN withdrawal_statuses ws ON ws.id = cw.withdrawal_status_id
  WHERE ws.value = 'PENDING'
)
SELECT
  -- どの業務種別の滞留か。
  pt.operation_type,
  -- JPY / BTC などの通貨コード。
  c.code AS currency_code,
  -- 現在時刻との差分から滞留帯を分類する。
  CASE
    WHEN TIMESTAMPDIFF(MINUTE, pt.started_at, CURRENT_TIMESTAMP) < 60 THEN 'UNDER_1_HOUR'
    WHEN TIMESTAMPDIFF(HOUR, pt.started_at, CURRENT_TIMESTAMP) < 24 THEN 'UNDER_24_HOURS'
    ELSE 'OVER_24_HOURS'
  END AS backlog_bucket,
  -- その帯に属する件数。
  COUNT(*) AS backlog_count,
  -- その帯で最も古い処理開始時刻。
  MIN(pt.started_at) AS oldest_started_at,
  -- その帯で最も長い滞留分数。
  MAX(TIMESTAMPDIFF(MINUTE, pt.started_at, CURRENT_TIMESTAMP)) AS max_pending_minutes
FROM pending_transactions pt
INNER JOIN currencies c ON c.id = pt.currency_id
GROUP BY
  -- 業務種別 x 通貨 x 滞留帯でまとめる。
  pt.operation_type,
  c.code,
  CASE
    WHEN TIMESTAMPDIFF(MINUTE, pt.started_at, CURRENT_TIMESTAMP) < 60 THEN 'UNDER_1_HOUR'
    WHEN TIMESTAMPDIFF(HOUR, pt.started_at, CURRENT_TIMESTAMP) < 24 THEN 'UNDER_24_HOURS'
    ELSE 'OVER_24_HOURS'
  END
-- ダッシュボードで見やすいよう、業務種別 -> 通貨 -> 滞留帯で並べる。
ORDER BY pt.operation_type, currency_code, backlog_bucket;
