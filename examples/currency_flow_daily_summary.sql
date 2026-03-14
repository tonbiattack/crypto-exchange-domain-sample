USE exchange_domain;

/*
  目的:
  - 通貨別の日次流入・流出・純流入を確認し、資金偏りを俯瞰する。
  - 法定通貨と暗号資産を同じ見た目で扱えるようにする。

  見方:
  - normalized_flows で各テーブルの完了イベントを
    (currency_id, activity_date, direction, amount) へ正規化してから集計する。
  - たとえば JPY の入金 1,000 と出金 400 が同日にあれば、
    total_in_amount=1000 / total_out_amount=400 / net_amount=600 になる。
*/
WITH normalized_flows AS (
  -- 法定入金の完了分を「流入(IN)」として取り込む。
  SELECT fd.currency_id, DATE(fd.completed_at) AS activity_date, 'IN' AS direction, fd.amount
  FROM fiat_deposits fd
  INNER JOIN deposit_statuses ds ON ds.id = fd.deposit_status_id
  WHERE ds.value = 'COMPLETED' AND fd.completed_at IS NOT NULL

  UNION ALL

  -- 法定出金の完了分を「流出(OUT)」として取り込む。
  SELECT fw.currency_id, DATE(fw.completed_at) AS activity_date, 'OUT' AS direction, fw.amount
  FROM fiat_withdrawals fw
  INNER JOIN withdrawal_statuses ws ON ws.id = fw.withdrawal_status_id
  WHERE ws.value = 'COMPLETED' AND fw.completed_at IS NOT NULL

  UNION ALL

  -- 暗号資産入金は confirmed_at を業務上の完了時刻として日次化する。
  SELECT cd.currency_id, DATE(cd.confirmed_at) AS activity_date, 'IN' AS direction, cd.amount
  FROM crypto_deposits cd
  INNER JOIN deposit_statuses ds ON ds.id = cd.deposit_status_id
  WHERE ds.value = 'COMPLETED' AND cd.confirmed_at IS NOT NULL

  UNION ALL

  -- 暗号資産出金の完了分を「流出(OUT)」として取り込む。
  SELECT cw.currency_id, DATE(cw.completed_at) AS activity_date, 'OUT' AS direction, cw.amount
  FROM crypto_withdrawals cw
  INNER JOIN withdrawal_statuses ws ON ws.id = cw.withdrawal_status_id
  WHERE ws.value = 'COMPLETED' AND cw.completed_at IS NOT NULL
)
SELECT
  -- どの日の資金移動か。
  nf.activity_date,
  -- JPY / BTC / ETH などの通貨コード。
  c.code AS currency_code,
  -- direction='IN' の金額だけを合計した日次流入額。
  SUM(CASE WHEN nf.direction = 'IN' THEN nf.amount ELSE 0 END) AS total_in_amount,
  -- direction='OUT' の金額だけを合計した日次流出額。
  SUM(CASE WHEN nf.direction = 'OUT' THEN nf.amount ELSE 0 END) AS total_out_amount,
  -- 流入を加算、流出を減算して算出した純流入額。
  -- 例: IN=1000, OUT=400 なら 1000 - 400 = 600。
  SUM(CASE WHEN nf.direction = 'IN' THEN nf.amount ELSE -nf.amount END) AS net_amount
FROM normalized_flows nf
INNER JOIN currencies c ON c.id = nf.currency_id
-- 通貨別・日別にまとめることで、同日の複数イベントを1行に圧縮する。
GROUP BY nf.activity_date, c.code
ORDER BY nf.activity_date DESC, c.code;
