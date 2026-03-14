USE exchange_domain;

/*
  目的:
  - 短期間に出金が集中しているユーザーを抽出し、疑わしい資金流出候補を見つける。
  - 法定通貨と暗号資産を同じ考え方で見られるようにする。

  想定シナリオ:
  - 直近24時間に完了した出金件数が3件以上。
  - 同期間の出金総額が入金総額を上回る、または入金がほぼない状態で連続出金している。

  注意点:
  - 通貨ごとに集計するため、異種通貨を単純合算しない。
  - CURRENT_TIMESTAMP 基準の監視SQLなので、実行時点によって結果は変わる。
*/
WITH completed_flows AS (
  SELECT
    -- 法定入金。
    fd.user_id,
    fd.currency_id,
    'IN' AS direction,
    fd.amount,
    fd.completed_at AS event_at
  FROM fiat_deposits fd
  INNER JOIN deposit_statuses ds ON ds.id = fd.deposit_status_id
  WHERE
    ds.value = 'COMPLETED'
    AND fd.completed_at >= DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 24 HOUR)

  UNION ALL

  SELECT
    -- 法定出金。
    fw.user_id,
    fw.currency_id,
    'OUT' AS direction,
    fw.amount,
    fw.completed_at AS event_at
  FROM fiat_withdrawals fw
  INNER JOIN withdrawal_statuses ws ON ws.id = fw.withdrawal_status_id
  WHERE
    ws.value = 'COMPLETED'
    AND fw.completed_at >= DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 24 HOUR)

  UNION ALL

  SELECT
    -- 暗号資産入金。
    cd.user_id,
    cd.currency_id,
    'IN' AS direction,
    cd.amount,
    cd.confirmed_at AS event_at
  FROM crypto_deposits cd
  INNER JOIN deposit_statuses ds ON ds.id = cd.deposit_status_id
  WHERE
    ds.value = 'COMPLETED'
    AND cd.confirmed_at >= DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 24 HOUR)

  UNION ALL

  SELECT
    -- 暗号資産出金。
    cw.user_id,
    cw.currency_id,
    'OUT' AS direction,
    cw.amount,
    cw.completed_at AS event_at
  FROM crypto_withdrawals cw
  INNER JOIN withdrawal_statuses ws ON ws.id = cw.withdrawal_status_id
  WHERE
    ws.value = 'COMPLETED'
    AND cw.completed_at >= DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 24 HOUR)
)
SELECT
  -- 対象ユーザー。
  u.id AS user_id,
  u.member_code,
  -- 通貨単位で監視する。
  c.code AS currency_code,
  -- 24時間内の入金件数。
  SUM(CASE WHEN cf.direction = 'IN' THEN 1 ELSE 0 END) AS inflow_count_24h,
  -- 24時間内の出金件数。
  SUM(CASE WHEN cf.direction = 'OUT' THEN 1 ELSE 0 END) AS outflow_count_24h,
  -- 24時間内の入金総額。
  SUM(CASE WHEN cf.direction = 'IN' THEN cf.amount ELSE 0 END) AS total_in_amount_24h,
  -- 24時間内の出金総額。
  SUM(CASE WHEN cf.direction = 'OUT' THEN cf.amount ELSE 0 END) AS total_out_amount_24h,
  -- 純流出超過額。
  SUM(CASE WHEN cf.direction = 'OUT' THEN cf.amount ELSE -cf.amount END) AS net_out_amount_24h,
  -- 最終イベント時刻。
  MAX(cf.event_at) AS last_event_at
FROM completed_flows cf
INNER JOIN users u ON u.id = cf.user_id
INNER JOIN currencies c ON c.id = cf.currency_id
GROUP BY
  u.id,
  u.member_code,
  c.code
HAVING
  -- 短期間の出金集中。
  SUM(CASE WHEN cf.direction = 'OUT' THEN 1 ELSE 0 END) >= 3
  AND (
    -- 出金総額が入金総額を上回る。
    SUM(CASE WHEN cf.direction = 'OUT' THEN cf.amount ELSE 0 END)
      > SUM(CASE WHEN cf.direction = 'IN' THEN cf.amount ELSE 0 END)
    OR
    -- そもそも入金がなく出金だけが集中している。
    SUM(CASE WHEN cf.direction = 'IN' THEN 1 ELSE 0 END) = 0
  )
ORDER BY
  -- 流出超過が大きい順に確認。
  net_out_amount_24h DESC,
  outflow_count_24h DESC,
  user_id;
