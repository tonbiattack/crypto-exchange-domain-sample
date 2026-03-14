USE exchange_domain;

/*
  目的:
  - ユーザー別・通貨別に入出金の純流入額を集計し、資金流入超過/流出超過の偏りを把握する。
  - 法定通貨と暗号資産を同じ見た目で俯瞰し、オペレーションやモニタリングの起点にする。

  設計意図:
  - 完了済みの入金/出金だけを対象にして、未確定データを純流量に混ぜない。
  - 4テーブルを共通列(user_id, currency_id, direction, amount, event_at)へ正規化してから集計する。
  - 通貨別に純額を見ることで、異種通貨間の単純合算を避ける。

  出力の読み方:
  - net_amount が正なら純流入、負なら純流出。
  - withdrawal_count が多く net_amount が大きく負のユーザーは資金流出モニタリング候補。
*/
WITH normalized_flows AS (
  SELECT
    -- 法定入金の完了データ。
    fd.user_id,
    fd.currency_id,
    'IN' AS direction,
    fd.amount,
    fd.completed_at AS event_at
  FROM fiat_deposits fd
  INNER JOIN deposit_statuses ds ON ds.id = fd.deposit_status_id
  WHERE ds.value = 'COMPLETED'

  UNION ALL

  SELECT
    -- 法定出金の完了データ。
    fw.user_id,
    fw.currency_id,
    'OUT' AS direction,
    fw.amount,
    fw.completed_at AS event_at
  FROM fiat_withdrawals fw
  INNER JOIN withdrawal_statuses ws ON ws.id = fw.withdrawal_status_id
  WHERE ws.value = 'COMPLETED'

  UNION ALL

  SELECT
    -- 暗号資産入金の完了データ。
    cd.user_id,
    cd.currency_id,
    'IN' AS direction,
    cd.amount,
    cd.confirmed_at AS event_at
  FROM crypto_deposits cd
  INNER JOIN deposit_statuses ds ON ds.id = cd.deposit_status_id
  WHERE ds.value = 'COMPLETED'

  UNION ALL

  SELECT
    -- 暗号資産出金の完了データ。
    cw.user_id,
    cw.currency_id,
    'OUT' AS direction,
    cw.amount,
    cw.completed_at AS event_at
  FROM crypto_withdrawals cw
  INNER JOIN withdrawal_statuses ws ON ws.id = cw.withdrawal_status_id
  WHERE ws.value = 'COMPLETED'
)
SELECT
  -- 会員特定に使う内部ID。
  u.id AS user_id,
  -- 業務向け表示コード。
  u.member_code,
  -- 通貨コード。
  c.code AS currency_code,
  -- 集計対象期間の先頭イベント。
  MIN(nf.event_at) AS first_event_at,
  -- 集計対象期間の末尾イベント。
  MAX(nf.event_at) AS last_event_at,
  -- 入金件数。
  SUM(CASE WHEN nf.direction = 'IN' THEN 1 ELSE 0 END) AS deposit_count,
  -- 出金件数。
  SUM(CASE WHEN nf.direction = 'OUT' THEN 1 ELSE 0 END) AS withdrawal_count,
  -- 総入金額。
  SUM(CASE WHEN nf.direction = 'IN' THEN nf.amount ELSE 0 END) AS total_in_amount,
  -- 総出金額。
  SUM(CASE WHEN nf.direction = 'OUT' THEN nf.amount ELSE 0 END) AS total_out_amount,
  -- 純流入額。
  SUM(CASE WHEN nf.direction = 'IN' THEN nf.amount ELSE -nf.amount END) AS net_amount
FROM normalized_flows nf
INNER JOIN users u ON u.id = nf.user_id
INNER JOIN currencies c ON c.id = nf.currency_id
GROUP BY
  u.id,
  u.member_code,
  c.code
ORDER BY
  -- 純流出が大きいユーザーを先頭にして監視しやすくする。
  net_amount ASC,
  withdrawal_count DESC,
  user_id;
