USE exchange_domain;

/*
  目的:
  - ユーザー別に取引アクティビティをランキング表示し、主要取引ユーザーを把握する。

  集計対象:
  - execution_count: 約定件数
  - total_base_volume: 約定数量の合計
  - total_notional_volume: 約定金額(価格*数量)の合計
  - first_executed_at / last_executed_at: 観測期間の開始/終了時刻

  設計意図:
  - trade_executions を基準に users と結合し、業務で使いやすい member_code を表示。
  - まずユーザー単位で集計し、外側で RANK() を使って順位付け。
  - 同率順位を許容するランキング仕様。

  使い方:
  - 期間を絞る場合は CTE 内 WHERE に executed_at 条件を追加する。
  - LIMIT を変更して上位件数を調整できる。
*/
WITH user_trade_agg AS (
  SELECT
    -- ランキング表示用の内部ユーザーID。
    u.id AS user_id,
    -- 業務画面で扱いやすい会員コード。
    u.member_code,
    -- 総約定件数。
    COUNT(*) AS execution_count,
    -- 総約定数量。
    SUM(te.executed_quantity) AS total_base_volume,
    -- 総名目出来高。
    SUM(te.executed_price * te.executed_quantity) AS total_notional_volume,
    -- 初回約定日時。
    MIN(te.executed_at) AS first_executed_at,
    -- 最終約定日時。
    MAX(te.executed_at) AS last_executed_at
  FROM trade_executions te
  -- ユーザー表示情報を載せるため結合。
  INNER JOIN users u ON u.id = te.user_id
  GROUP BY
    u.id,
    u.member_code
)
SELECT
  -- 名目出来高優先で同率順位を付与。
  RANK() OVER (
    ORDER BY
      total_notional_volume DESC,
      execution_count DESC,
      user_id ASC
  ) AS rank_no,
  user_id,
  member_code,
  execution_count,
  total_base_volume,
  total_notional_volume,
  first_executed_at,
  last_executed_at
FROM user_trade_agg
ORDER BY
  -- ランク順に上位から表示。
  rank_no,
  -- 同順位内の表示順を安定化。
  user_id
LIMIT 50;
