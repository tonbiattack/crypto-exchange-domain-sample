USE exchange_domain;

/*
  目的:
  - 理論残高差分を、外部入出金、約定、手数料のどれが主因かまで掘り下げて確認する。
  - user_balance_reconciliation_gap.sql の調査着手用の補助版。

  見方:
  - total_event_count は該当通貨に影響したイベント総数。
  - max_external_impact / max_trade_impact / max_fee_impact は各カテゴリの最大影響額。
  - dominant_cause は絶対値ベースで最も効いている要因。
*/
WITH balance_impacts AS (
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
  FROM trade_executions te
)
SELECT
  u.id AS user_id,
  u.member_code,
  c.code AS currency_code,
  COUNT(*) AS total_event_count,
  ROUND(SUM(bi.external_amount), 18) AS external_net_amount,
  ROUND(SUM(bi.trade_amount), 18) AS trade_net_amount,
  ROUND(SUM(bi.fee_amount), 18) AS fee_amount,
  ROUND(SUM(bi.external_amount) + SUM(bi.trade_amount) - SUM(bi.fee_amount), 18) AS theoretical_balance_delta,
  MAX(ABS(bi.external_amount)) AS max_external_impact,
  MAX(ABS(bi.trade_amount)) AS max_trade_impact,
  MAX(ABS(bi.fee_amount)) AS max_fee_impact,
  CASE
    WHEN MAX(ABS(bi.external_amount)) >= MAX(ABS(bi.trade_amount))
      AND MAX(ABS(bi.external_amount)) >= MAX(ABS(bi.fee_amount)) THEN 'EXTERNAL_FLOW'
    WHEN MAX(ABS(bi.trade_amount)) >= MAX(ABS(bi.external_amount))
      AND MAX(ABS(bi.trade_amount)) >= MAX(ABS(bi.fee_amount)) THEN 'TRADE_FLOW'
    ELSE 'FEE'
  END AS dominant_cause
FROM balance_impacts bi
INNER JOIN users u ON u.id = bi.user_id
INNER JOIN currencies c ON c.id = bi.currency_id
GROUP BY u.id, u.member_code, c.code
HAVING theoretical_balance_delta <> 0
ORDER BY ABS(theoretical_balance_delta) DESC, u.id, c.code;
