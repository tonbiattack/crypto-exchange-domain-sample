USE exchange_domain;

/*
  目的:
  - 同一ユーザー・同一通貨で、入金直後に大部分を短時間で出金している候補を抽出する。
  - いわゆる rapid movement / quick outflow の一次スクリーニングに使う。

  想定シナリオ:
  - 入金完了から24時間以内に、入金額の80%以上を出金している。
  - 通貨をまたぐ評価は避け、同一通貨内でのみ判定する。

  注意点:
  - あくまで候補抽出であり、不正確定ではない。
  - 閾値(24時間、80%)は業務ルールに合わせて調整する。
*/
WITH completed_inflows AS (
  SELECT
    -- 法定入金の完了分。
    fd.id AS event_id,
    'FIAT_DEPOSIT' AS event_type,
    fd.user_id,
    fd.currency_id,
    fd.amount,
    fd.completed_at AS completed_at
  FROM fiat_deposits fd
  INNER JOIN deposit_statuses ds ON ds.id = fd.deposit_status_id
  WHERE ds.value = 'COMPLETED'

  UNION ALL

  SELECT
    -- 暗号資産入金の完了分。
    cd.id AS event_id,
    'CRYPTO_DEPOSIT' AS event_type,
    cd.user_id,
    cd.currency_id,
    cd.amount,
    cd.confirmed_at AS completed_at
  FROM crypto_deposits cd
  INNER JOIN deposit_statuses ds ON ds.id = cd.deposit_status_id
  WHERE ds.value = 'COMPLETED'
),
completed_outflows AS (
  SELECT
    -- 法定出金の完了分。
    fw.id AS event_id,
    'FIAT_WITHDRAWAL' AS event_type,
    fw.user_id,
    fw.currency_id,
    fw.amount,
    fw.completed_at AS completed_at
  FROM fiat_withdrawals fw
  INNER JOIN withdrawal_statuses ws ON ws.id = fw.withdrawal_status_id
  WHERE ws.value = 'COMPLETED'

  UNION ALL

  SELECT
    -- 暗号資産出金の完了分。
    cw.id AS event_id,
    'CRYPTO_WITHDRAWAL' AS event_type,
    cw.user_id,
    cw.currency_id,
    cw.amount,
    cw.completed_at AS completed_at
  FROM crypto_withdrawals cw
  INNER JOIN withdrawal_statuses ws ON ws.id = cw.withdrawal_status_id
  WHERE ws.value = 'COMPLETED'
)
SELECT
  -- 候補ユーザー。
  u.id AS user_id,
  u.member_code,
  -- 判定通貨。
  c.code AS currency_code,
  -- 起点になった入金イベント情報。
  ci.event_type AS inflow_type,
  ci.event_id AS inflow_id,
  ci.completed_at AS inflow_completed_at,
  ci.amount AS inflow_amount,
  -- 24時間以内に発生した出金件数。
  COUNT(co.event_id) AS matched_outflow_count,
  -- 24時間以内の出金合計額。
  COALESCE(SUM(co.amount), 0) AS matched_outflow_amount,
  -- 出金/入金の比率。
  ROUND(COALESCE(SUM(co.amount), 0) / NULLIF(ci.amount, 0), 4) AS outflow_ratio
FROM completed_inflows ci
INNER JOIN users u ON u.id = ci.user_id
INNER JOIN currencies c ON c.id = ci.currency_id
LEFT JOIN completed_outflows co
  ON co.user_id = ci.user_id
  AND co.currency_id = ci.currency_id
  -- 入金完了後の出金だけを見る。
  AND co.completed_at >= ci.completed_at
  -- 24時間以内の出金に限定する。
  AND co.completed_at < DATE_ADD(ci.completed_at, INTERVAL 24 HOUR)
GROUP BY
  u.id,
  u.member_code,
  c.code,
  ci.event_type,
  ci.event_id,
  ci.completed_at,
  ci.amount
HAVING
  -- 出金が1件以上あり、
  COUNT(co.event_id) > 0
  -- 合計出金額が入金額の80%以上のものを候補にする。
  AND COALESCE(SUM(co.amount), 0) >= ci.amount * 0.8
ORDER BY
  outflow_ratio DESC,
  matched_outflow_amount DESC,
  inflow_completed_at DESC;
