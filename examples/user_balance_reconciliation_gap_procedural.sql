USE exchange_domain;

/*
  コピー元:
  - examples/user_balance_reconciliation_gap.sql

  元SQLの問題点:
  - 入出金、約定受払、手数料控除の全イベントを 1 つの UNION ALL へ押し込むと、
    差分理由の内訳や重要度判定を後から足しづらい。
  - 実務では「どの段階で差分が大きくなったか」を見たいので、
    イベント正規化と集約を分けた方が追跡しやすい。

  この手続き版でしていること:
  1. 残高影響イベントを temp table に正規化する。
  2. ユーザー x 通貨で集約する。
  3. 差分規模から severity_label / needs_investigation を後段で付与する。
*/
DROP TEMPORARY TABLE IF EXISTS tmp_balance_impacts;
CREATE TEMPORARY TABLE tmp_balance_impacts AS
SELECT fd.user_id, fd.currency_id, fd.completed_at AS occurred_at, fd.amount AS external_amount, 0 AS trade_amount, 0 AS fee_amount
FROM fiat_deposits fd
INNER JOIN deposit_statuses ds ON ds.id = fd.deposit_status_id
WHERE ds.value = 'COMPLETED' AND fd.completed_at IS NOT NULL

UNION ALL

SELECT fw.user_id, fw.currency_id, fw.completed_at AS occurred_at, -fw.amount AS external_amount, 0 AS trade_amount, 0 AS fee_amount
FROM fiat_withdrawals fw
INNER JOIN withdrawal_statuses ws ON ws.id = fw.withdrawal_status_id
WHERE ws.value = 'COMPLETED' AND fw.completed_at IS NOT NULL

UNION ALL

SELECT cd.user_id, cd.currency_id, cd.confirmed_at AS occurred_at, cd.amount AS external_amount, 0 AS trade_amount, 0 AS fee_amount
FROM crypto_deposits cd
INNER JOIN deposit_statuses ds ON ds.id = cd.deposit_status_id
WHERE ds.value = 'COMPLETED' AND cd.confirmed_at IS NOT NULL

UNION ALL

SELECT cw.user_id, cw.currency_id, cw.completed_at AS occurred_at, -cw.amount AS external_amount, 0 AS trade_amount, 0 AS fee_amount
FROM crypto_withdrawals cw
INNER JOIN withdrawal_statuses ws ON ws.id = cw.withdrawal_status_id
WHERE ws.value = 'COMPLETED' AND cw.completed_at IS NOT NULL

UNION ALL

SELECT te.user_id, te.from_currency_id AS currency_id, te.executed_at AS occurred_at, 0 AS external_amount, -te.from_amount AS trade_amount, 0 AS fee_amount
FROM trade_executions te

UNION ALL

SELECT te.user_id, te.to_currency_id AS currency_id, te.executed_at AS occurred_at, 0 AS external_amount, te.to_amount AS trade_amount, 0 AS fee_amount
FROM trade_executions te

UNION ALL

SELECT te.user_id, te.fee_currency_id AS currency_id, te.executed_at AS occurred_at, 0 AS external_amount, 0 AS trade_amount, te.fee_amount AS fee_amount
FROM trade_executions te;

DROP TEMPORARY TABLE IF EXISTS tmp_balance_rollups;
CREATE TEMPORARY TABLE tmp_balance_rollups AS
SELECT
  bi.user_id,
  bi.currency_id,
  MIN(bi.occurred_at) AS first_event_at,
  MAX(bi.occurred_at) AS last_event_at,
  ROUND(SUM(bi.external_amount), 18) AS external_net_amount,
  ROUND(SUM(bi.trade_amount), 18) AS trade_net_amount,
  ROUND(SUM(bi.fee_amount), 18) AS fee_amount,
  ROUND(SUM(bi.external_amount) + SUM(bi.trade_amount) - SUM(bi.fee_amount), 18) AS theoretical_balance_delta,
  CASE
    WHEN ABS(ROUND(SUM(bi.external_amount) + SUM(bi.trade_amount) - SUM(bi.fee_amount), 18)) >= 1000000 THEN 'CRITICAL'
    WHEN ABS(ROUND(SUM(bi.external_amount) + SUM(bi.trade_amount) - SUM(bi.fee_amount), 18)) >= 100000 THEN 'HIGH'
    ELSE 'NORMAL'
  END AS severity_label,
  CASE
    WHEN ABS(ROUND(SUM(bi.external_amount) + SUM(bi.trade_amount) - SUM(bi.fee_amount), 18)) >= 100000 THEN 1
    ELSE 0
  END AS needs_investigation
FROM tmp_balance_impacts bi
GROUP BY bi.user_id, bi.currency_id
HAVING theoretical_balance_delta <> 0;

-- RESULT_QUERY
SELECT
  u.id AS user_id,
  u.member_code,
  c.code AS currency_code,
  r.first_event_at,
  r.last_event_at,
  r.external_net_amount,
  r.trade_net_amount,
  r.fee_amount,
  r.theoretical_balance_delta,
  r.severity_label,
  r.needs_investigation
FROM tmp_balance_rollups r
INNER JOIN users u ON u.id = r.user_id
INNER JOIN currencies c ON c.id = r.currency_id
ORDER BY ABS(r.theoretical_balance_delta) DESC, u.id, c.code;
