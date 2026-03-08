USE exchange_domain;

/*
  目的:
  - 約定データを通貨ペア別・日別に集計し、どのペアに流動性が集まっているかを確認する。

  集計項目:
  - execution_count: 約定件数
  - base_volume: executed_quantity の合計(約定数量)
  - notional_volume: executed_price * executed_quantity の合計(約定金額)

  設計意図:
  - trade_executions は通貨ID保持のため、currencies を2回 JOIN して可読な通貨コードへ変換。
  - ORDER BY は「日付 -> 件数降順」にして、各日の主要ペアを先頭で見られるようにする。

  注意点:
  - from_currency/to_currency は「交換元/交換先」の定義に依存する。
  - BUY/SELL を分けて深掘りしたい場合は trading_orders と JOIN して side 別集計を追加する。
*/
SELECT
  DATE(te.executed_at) AS traded_date,
  cf.code AS from_currency,
  ct.code AS to_currency,
  COUNT(*) AS execution_count,
  SUM(te.executed_quantity) AS base_volume,
  SUM(te.executed_price * te.executed_quantity) AS notional_volume
FROM trade_executions te
INNER JOIN currencies cf ON cf.id = te.from_currency_id
INNER JOIN currencies ct ON ct.id = te.to_currency_id
GROUP BY
  DATE(te.executed_at),
  cf.code,
  ct.code
ORDER BY
  traded_date,
  execution_count DESC,
  from_currency,
  to_currency;
