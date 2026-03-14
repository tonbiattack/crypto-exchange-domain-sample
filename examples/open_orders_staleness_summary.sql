USE exchange_domain;

/*
  目的:
  - 未約定/未取消の OPEN 注文がどれだけ滞留しているかを把握する。
  - マッチング不全や価格乖離で残り続ける注文の棚卸しに使う。
*/
SELECT
  -- 滞留時間帯。
  CASE
    WHEN TIMESTAMPDIFF(MINUTE, o.placed_at, CURRENT_TIMESTAMP) < 60 THEN 'UNDER_1_HOUR'
    WHEN TIMESTAMPDIFF(HOUR, o.placed_at, CURRENT_TIMESTAMP) < 24 THEN 'UNDER_24_HOURS'
    WHEN TIMESTAMPDIFF(DAY, o.placed_at, CURRENT_TIMESTAMP) < 7 THEN 'UNDER_7_DAYS'
    ELSE 'OVER_7_DAYS'
  END AS staleness_bucket,
  -- 件数。
  COUNT(*) AS open_order_count,
  -- 最古注文時刻。
  MIN(o.placed_at) AS oldest_order_at,
  -- 最新注文時刻。
  MAX(o.placed_at) AS newest_order_at,
  -- 平均価格。
  AVG(o.price) AS avg_price,
  -- 平均数量。
  AVG(o.quantity) AS avg_quantity
FROM trading_orders o
INNER JOIN order_statuses os ON os.id = o.order_status_id
WHERE os.value = 'OPEN'
GROUP BY
  CASE
    WHEN TIMESTAMPDIFF(MINUTE, o.placed_at, CURRENT_TIMESTAMP) < 60 THEN 'UNDER_1_HOUR'
    WHEN TIMESTAMPDIFF(HOUR, o.placed_at, CURRENT_TIMESTAMP) < 24 THEN 'UNDER_24_HOURS'
    WHEN TIMESTAMPDIFF(DAY, o.placed_at, CURRENT_TIMESTAMP) < 7 THEN 'UNDER_7_DAYS'
    ELSE 'OVER_7_DAYS'
  END
ORDER BY
  CASE staleness_bucket
    WHEN 'UNDER_1_HOUR' THEN 1
    WHEN 'UNDER_24_HOURS' THEN 2
    WHEN 'UNDER_7_DAYS' THEN 3
    ELSE 4
  END;
