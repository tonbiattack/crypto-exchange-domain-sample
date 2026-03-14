USE exchange_domain;

/*
  目的:
  - PENDING のまま一定時間を超えて滞留している入出金を一覧化する。
  - オペレーション詰まりや外部接続障害の初期切り分けに使う。

  見方:
  - started_at は滞留開始時刻で、法定/暗号資産入金は requested_at / detected_at、
    出金は requested_at を使う。
  - pending_minutes が大きいほど、長時間処理中に留まっている。
*/
WITH pending_transactions AS (
  -- 法定入金の処理中案件。
  SELECT 'FIAT_DEPOSIT' AS operation_type, fd.id AS transaction_id, fd.user_id, fd.currency_id, ds.value AS status_value, fd.requested_at AS started_at, fd.amount
  FROM fiat_deposits fd
  INNER JOIN deposit_statuses ds ON ds.id = fd.deposit_status_id
  WHERE ds.value = 'PENDING'

  UNION ALL

  -- 法定出金の処理中案件。
  SELECT 'FIAT_WITHDRAWAL' AS operation_type, fw.id AS transaction_id, fw.user_id, fw.currency_id, ws.value AS status_value, fw.requested_at AS started_at, fw.amount
  FROM fiat_withdrawals fw
  INNER JOIN withdrawal_statuses ws ON ws.id = fw.withdrawal_status_id
  WHERE ws.value = 'PENDING'

  UNION ALL

  -- 暗号資産入金は detected_at から未完了時間を測る。
  SELECT 'CRYPTO_DEPOSIT' AS operation_type, cd.id AS transaction_id, cd.user_id, cd.currency_id, ds.value AS status_value, cd.detected_at AS started_at, cd.amount
  FROM crypto_deposits cd
  INNER JOIN deposit_statuses ds ON ds.id = cd.deposit_status_id
  WHERE ds.value = 'PENDING'

  UNION ALL

  -- 暗号資産出金の処理中案件。
  SELECT 'CRYPTO_WITHDRAWAL' AS operation_type, cw.id AS transaction_id, cw.user_id, cw.currency_id, ws.value AS status_value, cw.requested_at AS started_at, cw.amount
  FROM crypto_withdrawals cw
  INNER JOIN withdrawal_statuses ws ON ws.id = cw.withdrawal_status_id
  WHERE ws.value = 'PENDING'
)
SELECT
  -- どの業務種別のどの案件か。
  pt.operation_type,
  pt.transaction_id,
  pt.user_id,
  u.member_code,
  c.code AS currency_code,
  pt.status_value,
  pt.started_at,
  -- 開始時刻から現在時刻までの滞留分数。
  TIMESTAMPDIFF(MINUTE, pt.started_at, CURRENT_TIMESTAMP) AS pending_minutes,
  -- 対象金額/数量。
  pt.amount
FROM pending_transactions pt
INNER JOIN users u ON u.id = pt.user_id
INNER JOIN currencies c ON c.id = pt.currency_id
-- 60分未満は通常処理の揺らぎとして見なし、長時間滞留だけを出す。
WHERE TIMESTAMPDIFF(MINUTE, pt.started_at, CURRENT_TIMESTAMP) >= 60
ORDER BY pending_minutes DESC, pt.operation_type, pt.transaction_id;
