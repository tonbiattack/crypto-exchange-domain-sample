USE exchange_domain;

/*
  目的:
  - 高額な暗号資産入金のうち、その後の売却や出金が一定期間見られないものを抽出する。
  - 大口資金の滞留や、通常と異なる資金移動パターンの確認に使う。

  見方:
  - completed な crypto_deposits のうち amount >= 1 を「高額」とみなす。
  - confirmed_at から 7日以内に
    1. 同一通貨を交換元にする約定
    2. 同一通貨の completed 出金
    が無ければ候補として返す。
*/
SELECT
  -- どの高額暗号資産入金か。
  cd.id AS crypto_deposit_id,
  u.id AS user_id,
  u.member_code,
  c.code AS currency_code,
  cd.amount,
  cd.confirmed_at,
  -- 入金後 7日以内に同一通貨を売却した約定件数。
  (
    SELECT COUNT(*)
    FROM trade_executions te
    WHERE te.user_id = cd.user_id
      AND te.from_currency_id = cd.currency_id
      AND te.executed_at >= cd.confirmed_at
      AND te.executed_at < DATE_ADD(cd.confirmed_at, INTERVAL 7 DAY)
  ) AS matched_sell_execution_count,
  -- 入金後 7日以内に同一通貨で出金した件数。
  (
    SELECT COUNT(*)
    FROM crypto_withdrawals cw
    INNER JOIN withdrawal_statuses ws2 ON ws2.id = cw.withdrawal_status_id
    WHERE cw.user_id = cd.user_id
      AND cw.currency_id = cd.currency_id
      AND ws2.value = 'COMPLETED'
      AND cw.completed_at IS NOT NULL
      AND cw.completed_at >= cd.confirmed_at
      AND cw.completed_at < DATE_ADD(cd.confirmed_at, INTERVAL 7 DAY)
  ) AS matched_withdrawal_count
FROM crypto_deposits cd
INNER JOIN users u ON u.id = cd.user_id
INNER JOIN currencies c ON c.id = cd.currency_id
INNER JOIN deposit_statuses ds ON ds.id = cd.deposit_status_id
-- 完了済みかつ高額な暗号資産入金だけを対象にする。
WHERE ds.value = 'COMPLETED'
  AND cd.confirmed_at IS NOT NULL
  AND cd.amount >= 1
  -- 売却約定が1件でもあれば「未対応」ではないので除外する。
  AND NOT EXISTS (
    SELECT 1
    FROM trade_executions te
    WHERE te.user_id = cd.user_id
      AND te.from_currency_id = cd.currency_id
      AND te.executed_at >= cd.confirmed_at
      AND te.executed_at < DATE_ADD(cd.confirmed_at, INTERVAL 7 DAY)
  )
  -- 完了済み出金が1件でもあれば「未対応」ではないので除外する。
  AND NOT EXISTS (
    SELECT 1
    FROM crypto_withdrawals cw
    INNER JOIN withdrawal_statuses ws ON ws.id = cw.withdrawal_status_id
    WHERE cw.user_id = cd.user_id
      AND cw.currency_id = cd.currency_id
      AND ws.value = 'COMPLETED'
      AND cw.completed_at IS NOT NULL
      AND cw.completed_at >= cd.confirmed_at
      AND cw.completed_at < DATE_ADD(cd.confirmed_at, INTERVAL 7 DAY)
  )
-- より高額で新しいものから確認できる順。
ORDER BY cd.amount DESC, cd.confirmed_at DESC, cd.id;
