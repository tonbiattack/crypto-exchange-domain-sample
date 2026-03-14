USE exchange_domain;

/*
  目的:
  - 複数ユーザーに再利用されている出金先アドレスを抽出し、不正送金ネットワークの初期探索に使う。
*/
SELECT
  -- 使い回し候補の送金先アドレス。
  cw.destination_address,
  -- 通貨をまたがず BTC/ETH/XRP ごとに分ける。
  c.code AS currency_code,
  -- そのアドレス向けの総出金件数。
  COUNT(*) AS withdrawal_count,
  -- そのアドレスを使ったユーザー数。
  COUNT(DISTINCT cw.user_id) AS user_count,
  -- 初回使用日時。
  MIN(cw.requested_at) AS first_requested_at,
  -- 最終使用日時。
  MAX(cw.requested_at) AS last_requested_at
FROM crypto_withdrawals cw
INNER JOIN currencies c ON c.id = cw.currency_id
GROUP BY cw.destination_address, c.code
-- 「同一アドレスを2人以上が使っている」ものだけを候補化する。
HAVING COUNT(DISTINCT cw.user_id) >= 2
ORDER BY user_count DESC, withdrawal_count DESC, last_requested_at DESC;
