USE exchange_domain;

/*
  目的:
  - 入金後に取引で別通貨へ転換し、その後に出金まで至った多段フロー候補を抽出する。
  - 単純な「入金直後の同通貨出金」ではなく、通貨変換を挟んだ資金移動の一次確認に使う。

  見方:
  - completed な fiat_deposits / crypto_deposits を起点入金とする。
  - 入金後 24 時間以内の trade_executions を intermediate_trade として結び付ける。
  - さらにその約定後 24 時間以内の completed 出金を final_outflow として結び付ける。
  - final_outflow_currency_code が intermediate_to_currency_code と一致していれば、
    入金 -> 取引 -> 出金 の多段移動が成立している。

  注意点:
  - 24時間、80% は例示閾値。実務ではアプリ側パラメータ化を推奨。
  - さらに複数約定をまたぐルート探索や外部チェーン分析まで行うなら、手続き型コードで補完した方がよい。
*/
WITH completed_inflows AS (
  SELECT
    'FIAT_DEPOSIT' AS inflow_type,
    fd.id AS inflow_id,
    fd.user_id,
    fd.currency_id AS inflow_currency_id,
    fd.amount AS inflow_amount,
    fd.completed_at AS inflow_completed_at
  FROM fiat_deposits fd
  INNER JOIN deposit_statuses ds ON ds.id = fd.deposit_status_id
  WHERE ds.value = 'COMPLETED'
    AND fd.completed_at IS NOT NULL

  UNION ALL

  SELECT
    'CRYPTO_DEPOSIT' AS inflow_type,
    cd.id AS inflow_id,
    cd.user_id,
    cd.currency_id AS inflow_currency_id,
    cd.amount AS inflow_amount,
    cd.confirmed_at AS inflow_completed_at
  FROM crypto_deposits cd
  INNER JOIN deposit_statuses ds ON ds.id = cd.deposit_status_id
  WHERE ds.value = 'COMPLETED'
    AND cd.confirmed_at IS NOT NULL
),
completed_outflows AS (
  SELECT
    'FIAT_WITHDRAWAL' AS outflow_type,
    fw.id AS outflow_id,
    fw.user_id,
    fw.currency_id AS outflow_currency_id,
    fw.amount AS outflow_amount,
    fw.completed_at AS outflow_completed_at
  FROM fiat_withdrawals fw
  INNER JOIN withdrawal_statuses ws ON ws.id = fw.withdrawal_status_id
  WHERE ws.value = 'COMPLETED'
    AND fw.completed_at IS NOT NULL

  UNION ALL

  SELECT
    'CRYPTO_WITHDRAWAL' AS outflow_type,
    cw.id AS outflow_id,
    cw.user_id,
    cw.currency_id AS outflow_currency_id,
    cw.amount AS outflow_amount,
    cw.completed_at AS outflow_completed_at
  FROM crypto_withdrawals cw
  INNER JOIN withdrawal_statuses ws ON ws.id = cw.withdrawal_status_id
  WHERE ws.value = 'COMPLETED'
    AND cw.completed_at IS NOT NULL
)
SELECT
  u.id AS user_id,
  u.member_code,
  ci.inflow_type,
  ci.inflow_id,
  inflow_currency.code AS inflow_currency_code,
  ci.inflow_amount,
  ci.inflow_completed_at,
  te.id AS trade_execution_id,
  intermediate_from_currency.code AS intermediate_from_currency_code,
  intermediate_to_currency.code AS intermediate_to_currency_code,
  te.from_amount AS trade_from_amount,
  te.to_amount AS trade_to_amount,
  te.executed_at,
  co.outflow_type AS final_outflow_type,
  co.outflow_id AS final_outflow_id,
  final_outflow_currency.code AS final_outflow_currency_code,
  co.outflow_amount AS final_outflow_amount,
  co.outflow_completed_at,
  ROUND(co.outflow_amount / NULLIF(te.to_amount, 0), 4) AS outflow_vs_trade_ratio
FROM completed_inflows ci
INNER JOIN users u ON u.id = ci.user_id
INNER JOIN currencies inflow_currency ON inflow_currency.id = ci.inflow_currency_id
INNER JOIN trade_executions te
  ON te.user_id = ci.user_id
  AND te.executed_at >= ci.inflow_completed_at
  AND te.executed_at < DATE_ADD(ci.inflow_completed_at, INTERVAL 24 HOUR)
INNER JOIN currencies intermediate_from_currency ON intermediate_from_currency.id = te.from_currency_id
INNER JOIN currencies intermediate_to_currency ON intermediate_to_currency.id = te.to_currency_id
INNER JOIN completed_outflows co
  ON co.user_id = ci.user_id
  AND co.outflow_currency_id = te.to_currency_id
  AND co.outflow_completed_at >= te.executed_at
  AND co.outflow_completed_at < DATE_ADD(te.executed_at, INTERVAL 24 HOUR)
INNER JOIN currencies final_outflow_currency ON final_outflow_currency.id = co.outflow_currency_id
WHERE te.from_currency_id = ci.inflow_currency_id
  AND co.outflow_amount >= te.to_amount * 0.8
ORDER BY ci.inflow_completed_at DESC, te.executed_at DESC, co.outflow_completed_at DESC;
