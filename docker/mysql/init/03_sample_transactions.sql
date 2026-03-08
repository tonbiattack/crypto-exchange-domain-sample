USE exchange_domain;
SET NAMES utf8mb4;
SET SESSION cte_max_recursion_depth = 10000;

START TRANSACTION;

INSERT INTO currencies (code, name, currency_type, description) VALUES
  ('JPY', 'Japanese Yen', 'FIAT', 'Japanese fiat currency'),
  ('BTC', 'Bitcoin', 'CRYPTO', 'Bitcoin network asset'),
  ('ETH', 'Ethereum', 'CRYPTO', 'Ethereum network asset'),
  ('USDT', 'Tether USD', 'CRYPTO', 'USDT stablecoin'),
  ('XRP', 'XRP', 'CRYPTO', 'XRP Ledger asset')
ON DUPLICATE KEY UPDATE
  name = VALUES(name),
  currency_type = VALUES(currency_type),
  description = VALUES(description),
  updated_at = CURRENT_TIMESTAMP(6);

SET @status_active_id := (SELECT id FROM user_statuses WHERE value = 'ACTIVE' LIMIT 1);
SET @occupation_other_id := (SELECT id FROM occupations WHERE value = 'OTHER' LIMIT 1);
SET @income_mid_id := (SELECT id FROM annual_income_brackets WHERE value = '5M_TO_7M' LIMIT 1);
SET @asset_mid_id := (SELECT id FROM financial_asset_brackets WHERE value = '5M_TO_10M' LIMIT 1);
SET @order_status_filled_id := (SELECT id FROM order_statuses WHERE value = 'FILLED' LIMIT 1);
SET @order_status_open_id := (SELECT id FROM order_statuses WHERE value = 'OPEN' LIMIT 1);
SET @order_status_cancelled_id := (SELECT id FROM order_statuses WHERE value = 'CANCELLED' LIMIT 1);
SET @deposit_completed_id := (SELECT id FROM deposit_statuses WHERE value = 'COMPLETED' LIMIT 1);
SET @deposit_failed_id := (SELECT id FROM deposit_statuses WHERE value = 'FAILED' LIMIT 1);
SET @withdrawal_completed_id := (SELECT id FROM withdrawal_statuses WHERE value = 'COMPLETED' LIMIT 1);
SET @withdrawal_failed_id := (SELECT id FROM withdrawal_statuses WHERE value = 'FAILED' LIMIT 1);

SET @jpy_id := (SELECT id FROM currencies WHERE code = 'JPY' LIMIT 1);
SET @btc_id := (SELECT id FROM currencies WHERE code = 'BTC' LIMIT 1);
SET @eth_id := (SELECT id FROM currencies WHERE code = 'ETH' LIMIT 1);
SET @usdt_id := (SELECT id FROM currencies WHERE code = 'USDT' LIMIT 1);
SET @xrp_id := (SELECT id FROM currencies WHERE code = 'XRP' LIMIT 1);

INSERT INTO users (public_user_hash, member_code, current_status_id, registered_at)
WITH RECURSIVE seq_users(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM seq_users WHERE n < 300
)
SELECT
  SHA2(CONCAT('sample-user-', n), 256),
  CONCAT('M', LPAD(n, 10, '0')),
  @status_active_id,
  DATE_ADD('2025-01-01 00:00:00', INTERVAL n DAY)
FROM seq_users
ON DUPLICATE KEY UPDATE
  current_status_id = VALUES(current_status_id),
  updated_at = CURRENT_TIMESTAMP(6);
INSERT INTO user_profile_versions (
  user_id, version_no, last_name, first_name, birth_date, country_code,
  occupation_id, annual_income_bracket_id, financial_asset_bracket_id,
  declared_at, change_reason
)
SELECT
  u.id,
  1,
  CONCAT('SampleLast', SUBSTRING(u.member_code, 2, 4)),
  CONCAT('SampleFirst', RIGHT(u.member_code, 4)),
  DATE_SUB('2000-01-01', INTERVAL (u.id % 9000) DAY),
  'JP',
  @occupation_other_id,
  @income_mid_id,
  @asset_mid_id,
  DATE_ADD('2025-01-01 09:00:00', INTERVAL (u.id % 365) DAY),
  'initial onboarding snapshot'
FROM users u
LEFT JOIN user_profile_versions upv ON upv.user_id = u.id AND upv.version_no = 1
WHERE upv.id IS NULL;

INSERT INTO trading_orders (
  user_id, public_order_hash, side, order_type, from_currency_id, to_currency_id,
  price, quantity, order_status_id, placed_at, cancelled_at
)
WITH RECURSIVE seq_orders(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM seq_orders WHERE n < 3000
)
SELECT
  ((n - 1) % 300) + 1,
  SHA2(CONCAT('sample-order-', n), 256),
  CASE WHEN MOD(n, 2) = 0 THEN 'BUY' ELSE 'SELL' END,
  CASE WHEN MOD(n, 3) = 0 THEN 'MARKET' ELSE 'LIMIT' END,
  CASE MOD(n, 4) WHEN 0 THEN @jpy_id WHEN 1 THEN @btc_id WHEN 2 THEN @eth_id ELSE @usdt_id END,
  CASE MOD(n, 4) WHEN 0 THEN @btc_id WHEN 1 THEN @jpy_id WHEN 2 THEN @usdt_id ELSE @eth_id END,
  CAST((1000 + MOD(n, 50000)) / 10 AS DECIMAL(36, 18)),
  CAST((1 + MOD(n, 1000)) / 100 AS DECIMAL(36, 18)),
  CASE WHEN MOD(n, 10) < 7 THEN @order_status_filled_id WHEN MOD(n, 10) < 9 THEN @order_status_open_id ELSE @order_status_cancelled_id END,
  DATE_ADD('2026-01-01 00:00:00', INTERVAL n MINUTE),
  CASE WHEN MOD(n, 10) = 9 THEN DATE_ADD('2026-01-01 00:00:00', INTERVAL (n + 5) MINUTE) ELSE NULL END
FROM seq_orders
ON DUPLICATE KEY UPDATE
  order_status_id = VALUES(order_status_id),
  updated_at = CURRENT_TIMESTAMP(6);

INSERT INTO trade_executions (
  order_id, user_id, public_execution_hash, from_currency_id, to_currency_id,
  executed_price, executed_quantity, fee_currency_id, fee_amount, executed_at
)
WITH RECURSIVE seq_exec(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM seq_exec WHERE n < 4500
)
SELECT
  o.id,
  o.user_id,
  SHA2(CONCAT('sample-exec-', n), 256),
  o.from_currency_id,
  o.to_currency_id,
  CAST((1100 + MOD(n, 49000)) / 10 AS DECIMAL(36, 18)),
  CAST((1 + MOD(n, 300)) / 100 AS DECIMAL(36, 18)),
  CASE WHEN o.to_currency_id = @jpy_id THEN @jpy_id ELSE o.to_currency_id END,
  CAST((1 + MOD(n, 20)) / 1000 AS DECIMAL(36, 18)),
  DATE_ADD(o.placed_at, INTERVAL (1 + MOD(n, 10)) SECOND)
FROM seq_exec s
JOIN trading_orders o ON o.id = ((s.n - 1) % 3000) + 1
ON DUPLICATE KEY UPDATE
  executed_price = VALUES(executed_price);
INSERT INTO fiat_deposits (
  user_id, public_deposit_hash, currency_id, amount, deposit_status_id,
  requested_at, completed_at, failed_at
)
WITH RECURSIVE seq_fiat_dep(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM seq_fiat_dep WHERE n < 2000
)
SELECT
  ((n - 1) % 300) + 1,
  SHA2(CONCAT('sample-fiat-deposit-', n), 256),
  @jpy_id,
  CAST((10000 + MOD(n, 500000)) AS DECIMAL(36, 18)),
  CASE WHEN MOD(n, 20) = 0 THEN @deposit_failed_id ELSE @deposit_completed_id END,
  DATE_ADD('2026-01-01 00:00:00', INTERVAL n MINUTE),
  CASE WHEN MOD(n, 20) = 0 THEN NULL ELSE DATE_ADD('2026-01-01 00:00:00', INTERVAL (n + 10) MINUTE) END,
  CASE WHEN MOD(n, 20) = 0 THEN DATE_ADD('2026-01-01 00:00:00', INTERVAL (n + 7) MINUTE) ELSE NULL END
FROM seq_fiat_dep
ON DUPLICATE KEY UPDATE
  deposit_status_id = VALUES(deposit_status_id),
  updated_at = CURRENT_TIMESTAMP(6);

INSERT INTO fiat_withdrawals (
  user_id, public_withdrawal_hash, currency_id, amount, withdrawal_status_id,
  requested_at, completed_at, failed_at
)
WITH RECURSIVE seq_fiat_wd(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM seq_fiat_wd WHERE n < 1500
)
SELECT
  ((n - 1) % 300) + 1,
  SHA2(CONCAT('sample-fiat-withdrawal-', n), 256),
  @jpy_id,
  CAST((5000 + MOD(n, 300000)) AS DECIMAL(36, 18)),
  CASE WHEN MOD(n, 15) = 0 THEN @withdrawal_failed_id ELSE @withdrawal_completed_id END,
  DATE_ADD('2026-01-05 00:00:00', INTERVAL n MINUTE),
  CASE WHEN MOD(n, 15) = 0 THEN NULL ELSE DATE_ADD('2026-01-05 00:00:00', INTERVAL (n + 20) MINUTE) END,
  CASE WHEN MOD(n, 15) = 0 THEN DATE_ADD('2026-01-05 00:00:00', INTERVAL (n + 9) MINUTE) ELSE NULL END
FROM seq_fiat_wd
ON DUPLICATE KEY UPDATE
  withdrawal_status_id = VALUES(withdrawal_status_id),
  updated_at = CURRENT_TIMESTAMP(6);

INSERT INTO crypto_deposits (
  user_id, public_deposit_hash, currency_id, tx_hash, amount, deposit_status_id,
  detected_at, confirmed_at, failed_at
)
WITH RECURSIVE seq_crypto_dep(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM seq_crypto_dep WHERE n < 2500
)
SELECT
  ((n - 1) % 300) + 1,
  SHA2(CONCAT('sample-crypto-deposit-', n), 256),
  CASE MOD(n, 3) WHEN 0 THEN @btc_id WHEN 1 THEN @eth_id ELSE @xrp_id END,
  SHA2(CONCAT('sample-crypto-deposit-tx-', n), 256),
  CAST((1 + MOD(n, 10000)) / 10000 AS DECIMAL(36, 18)),
  CASE WHEN MOD(n, 18) = 0 THEN @deposit_failed_id ELSE @deposit_completed_id END,
  DATE_ADD('2026-01-10 00:00:00', INTERVAL n MINUTE),
  CASE WHEN MOD(n, 18) = 0 THEN NULL ELSE DATE_ADD('2026-01-10 00:00:00', INTERVAL (n + 30) MINUTE) END,
  CASE WHEN MOD(n, 18) = 0 THEN DATE_ADD('2026-01-10 00:00:00', INTERVAL (n + 12) MINUTE) ELSE NULL END
FROM seq_crypto_dep
ON DUPLICATE KEY UPDATE
  deposit_status_id = VALUES(deposit_status_id),
  updated_at = CURRENT_TIMESTAMP(6);
INSERT INTO crypto_withdrawals (
  user_id, public_withdrawal_hash, currency_id, destination_address, amount,
  tx_hash, withdrawal_status_id, requested_at, completed_at, failed_at
)
WITH RECURSIVE seq_crypto_wd(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM seq_crypto_wd WHERE n < 1800
)
SELECT
  ((n - 1) % 300) + 1,
  SHA2(CONCAT('sample-crypto-withdrawal-', n), 256),
  CASE MOD(n, 3) WHEN 0 THEN @btc_id WHEN 1 THEN @eth_id ELSE @xrp_id END,
  CONCAT('addr_', LPAD(n, 12, '0')),
  CAST((1 + MOD(n, 9000)) / 10000 AS DECIMAL(36, 18)),
  CASE WHEN MOD(n, 17) = 0 THEN NULL ELSE SHA2(CONCAT('sample-crypto-withdrawal-tx-', n), 256) END,
  CASE WHEN MOD(n, 17) = 0 THEN @withdrawal_failed_id ELSE @withdrawal_completed_id END,
  DATE_ADD('2026-01-15 00:00:00', INTERVAL n MINUTE),
  CASE WHEN MOD(n, 17) = 0 THEN NULL ELSE DATE_ADD('2026-01-15 00:00:00', INTERVAL (n + 25) MINUTE) END,
  CASE WHEN MOD(n, 17) = 0 THEN DATE_ADD('2026-01-15 00:00:00', INTERVAL (n + 11) MINUTE) ELSE NULL END
FROM seq_crypto_wd
ON DUPLICATE KEY UPDATE
  withdrawal_status_id = VALUES(withdrawal_status_id),
  updated_at = CURRENT_TIMESTAMP(6);

COMMIT;
