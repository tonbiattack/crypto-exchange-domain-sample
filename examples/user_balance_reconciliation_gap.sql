USE exchange_domain;

/*
  目的:
  - ユーザー別・通貨別に、入出金と約定から再計算した理論残高増減を確認する。
  - 実残高テーブルが無い環境でも、イベント起点で残高影響を棚卸しできるようにする。

  見方:
  - external_net_amount は入出金による純増減。
  - trade_net_amount は約定による受払増減。
  - fee_amount は手数料控除額。
  - theoretical_balance_delta = external_net_amount + trade_net_amount - fee_amount。
    例: JPY 入金 1,000,000、BTC 買付で JPY 500,000 消費、手数料 1,000 なら 499,000。
*/
WITH balance_impacts AS (
  -- 法定入金の完了分は、その通貨残高を外部要因で増加させる。
  SELECT fd.user_id, fd.currency_id, fd.completed_at AS occurred_at, fd.amount AS external_amount, 0 AS trade_amount, 0 AS fee_amount
  FROM fiat_deposits fd
  INNER JOIN deposit_statuses ds ON ds.id = fd.deposit_status_id
  WHERE ds.value = 'COMPLETED' AND fd.completed_at IS NOT NULL

  UNION ALL

  -- 法定出金の完了分は、その通貨残高を外部要因で減少させる。
  SELECT fw.user_id, fw.currency_id, fw.completed_at AS occurred_at, -fw.amount AS external_amount, 0 AS trade_amount, 0 AS fee_amount
  FROM fiat_withdrawals fw
  INNER JOIN withdrawal_statuses ws ON ws.id = fw.withdrawal_status_id
  WHERE ws.value = 'COMPLETED' AND fw.completed_at IS NOT NULL

  UNION ALL

  -- 暗号資産入金の完了分は、その通貨残高を外部要因で増加させる。
  SELECT cd.user_id, cd.currency_id, cd.confirmed_at AS occurred_at, cd.amount AS external_amount, 0 AS trade_amount, 0 AS fee_amount
  FROM crypto_deposits cd
  INNER JOIN deposit_statuses ds ON ds.id = cd.deposit_status_id
  WHERE ds.value = 'COMPLETED' AND cd.confirmed_at IS NOT NULL

  UNION ALL

  -- 暗号資産出金の完了分は、その通貨残高を外部要因で減少させる。
  SELECT cw.user_id, cw.currency_id, cw.completed_at AS occurred_at, -cw.amount AS external_amount, 0 AS trade_amount, 0 AS fee_amount
  FROM crypto_withdrawals cw
  INNER JOIN withdrawal_statuses ws ON ws.id = cw.withdrawal_status_id
  WHERE ws.value = 'COMPLETED' AND cw.completed_at IS NOT NULL

  UNION ALL

  -- 約定では交換元通貨が減る。
  -- from_amount を持たせたので、価格計算の再推定ではなく保存値を使う。
  SELECT te.user_id, te.from_currency_id AS currency_id, te.executed_at AS occurred_at, 0 AS external_amount, -te.from_amount AS trade_amount, 0 AS fee_amount
  FROM trade_executions te

  UNION ALL

  -- 約定では交換先通貨が増える。
  -- to_amount を持たせたので、約定数量の意味に依存せず保存値を使う。
  SELECT te.user_id, te.to_currency_id AS currency_id, te.executed_at AS occurred_at, 0 AS external_amount, te.to_amount AS trade_amount, 0 AS fee_amount
  FROM trade_executions te

  UNION ALL

  -- 手数料は fee_currency_id の残高を減らすので、trade_amount ではなく fee_amount として別管理する。
  SELECT te.user_id, te.fee_currency_id AS currency_id, te.executed_at AS occurred_at, 0 AS external_amount, 0 AS trade_amount, te.fee_amount AS fee_amount
  FROM trade_executions te
)
SELECT
  -- どのユーザーのどの通貨の残高影響か。
  u.id AS user_id,
  u.member_code,
  c.code AS currency_code,
  -- 最初と最後の残高影響イベント時刻。
  MIN(bi.occurred_at) AS first_event_at,
  MAX(bi.occurred_at) AS last_event_at,
  -- 入出金起因の純増減。
  ROUND(SUM(bi.external_amount), 18) AS external_net_amount,
  -- 約定起因の純増減。
  ROUND(SUM(bi.trade_amount), 18) AS trade_net_amount,
  -- 手数料控除額。
  ROUND(SUM(bi.fee_amount), 18) AS fee_amount,
  -- 理論残高増減。
  -- 例: external=1000000, trade=-500000, fee=1000 なら 499000。
  ROUND(SUM(bi.external_amount) + SUM(bi.trade_amount) - SUM(bi.fee_amount), 18) AS theoretical_balance_delta
FROM balance_impacts bi
INNER JOIN users u ON u.id = bi.user_id
INNER JOIN currencies c ON c.id = bi.currency_id
GROUP BY u.id, u.member_code, c.code
-- 変動がゼロの通貨は監査対象としての優先度が低いので除外する。
HAVING theoretical_balance_delta <> 0
ORDER BY ABS(theoretical_balance_delta) DESC, u.id, c.code;
