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
SET @deposit_pending_id := (SELECT id FROM deposit_statuses WHERE value = 'PENDING' LIMIT 1);
SET @deposit_completed_id := (SELECT id FROM deposit_statuses WHERE value = 'COMPLETED' LIMIT 1);
SET @deposit_failed_id := (SELECT id FROM deposit_statuses WHERE value = 'FAILED' LIMIT 1);
SET @withdrawal_pending_id := (SELECT id FROM withdrawal_statuses WHERE value = 'PENDING' LIMIT 1);
SET @withdrawal_completed_id := (SELECT id FROM withdrawal_statuses WHERE value = 'COMPLETED' LIMIT 1);
SET @withdrawal_failed_id := (SELECT id FROM withdrawal_statuses WHERE value = 'FAILED' LIMIT 1);
SET @actor_system_id := (SELECT id FROM actor_types WHERE value = 'SYSTEM' LIMIT 1);
SET @case_source_auto_id := (SELECT id FROM case_source_types WHERE value = 'AUTO' LIMIT 1);
SET @case_status_open_id := (SELECT id FROM case_statuses WHERE value = 'OPEN' LIMIT 1);
SET @case_status_investigating_id := (SELECT id FROM case_statuses WHERE value = 'INVESTIGATING' LIMIT 1);
SET @case_status_closed_id := (SELECT id FROM case_statuses WHERE value = 'CLOSED' LIMIT 1);
SET @risk_high_id := (SELECT id FROM risk_levels WHERE value = 'HIGH' LIMIT 1);
SET @risk_critical_id := (SELECT id FROM risk_levels WHERE value = 'CRITICAL' LIMIT 1);
SET @alert_status_open_id := (SELECT id FROM alert_event_statuses WHERE value = 'OPEN' LIMIT 1);
SET @alert_status_reviewed_id := (SELECT id FROM alert_event_statuses WHERE value = 'REVIEWED' LIMIT 1);
SET @freeze_action_type_id := (SELECT id FROM account_action_types WHERE value = 'FREEZE' LIMIT 1);
SET @frozen_event_type_id := (SELECT id FROM user_status_event_types WHERE value = 'FROZEN' LIMIT 1);
SET @status_frozen_id := (SELECT id FROM user_statuses WHERE value = 'FROZEN' LIMIT 1);

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

INSERT INTO user_profile_versions (
  user_id, version_no, last_name, first_name, birth_date, country_code,
  occupation_id, annual_income_bracket_id, financial_asset_bracket_id,
  declared_at, change_reason
)
SELECT
  1,
  2,
  'SampleLast0001',
  'SampleFirst0001',
  '1990-01-01',
  'JP',
  @occupation_other_id,
  @income_mid_id,
  @asset_mid_id,
  '2026-02-03 10:00:00',
  'annual income update'
FROM DUAL
WHERE NOT EXISTS (
  SELECT 1
  FROM user_profile_versions
  WHERE user_id = 1 AND version_no = 2
);

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
  executed_price, executed_quantity, from_amount, to_amount, fee_currency_id, fee_amount, executed_at
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
  CAST(((1100 + MOD(n, 49000)) / 10) * ((1 + MOD(n, 300)) / 100) AS DECIMAL(36, 18)),
  CAST((1 + MOD(n, 300)) / 100 AS DECIMAL(36, 18)),
  CASE WHEN o.to_currency_id = @jpy_id THEN @jpy_id ELSE o.to_currency_id END,
  CAST((1 + MOD(n, 20)) / 1000 AS DECIMAL(36, 18)),
  DATE_ADD(o.placed_at, INTERVAL (1 + MOD(n, 10)) SECOND)
FROM seq_exec s
JOIN trading_orders o ON o.id = ((s.n - 1) % 3000) + 1
ON DUPLICATE KEY UPDATE
  executed_price = VALUES(executed_price),
  from_amount = VALUES(from_amount),
  to_amount = VALUES(to_amount);
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

INSERT INTO crypto_withdrawals (
  user_id, public_withdrawal_hash, currency_id, destination_address, amount,
  tx_hash, withdrawal_status_id, requested_at, completed_at, failed_at
)
SELECT
  1,
  SHA2('sample-shared-address-withdrawal-user-1', 256),
  @btc_id,
  'shared_addr_monitoring_001',
  CAST(0.25000000 AS DECIMAL(36, 18)),
  SHA2('sample-shared-address-withdrawal-user-1-tx', 256),
  @withdrawal_completed_id,
  '2026-02-03 11:00:00',
  '2026-02-03 11:20:00',
  NULL
FROM DUAL
WHERE NOT EXISTS (
  SELECT 1
  FROM crypto_withdrawals
  WHERE public_withdrawal_hash = SHA2('sample-shared-address-withdrawal-user-1', 256)
);

INSERT INTO crypto_withdrawals (
  user_id, public_withdrawal_hash, currency_id, destination_address, amount,
  tx_hash, withdrawal_status_id, requested_at, completed_at, failed_at
)
SELECT
  2,
  SHA2('sample-shared-address-withdrawal-user-2', 256),
  @btc_id,
  'shared_addr_monitoring_001',
  CAST(0.35000000 AS DECIMAL(36, 18)),
  SHA2('sample-shared-address-withdrawal-user-2-tx', 256),
  @withdrawal_completed_id,
  '2026-02-03 12:00:00',
  '2026-02-03 12:25:00',
  NULL
FROM DUAL
WHERE NOT EXISTS (
  SELECT 1
  FROM crypto_withdrawals
  WHERE public_withdrawal_hash = SHA2('sample-shared-address-withdrawal-user-2', 256)
);

INSERT INTO crypto_withdrawals (
  user_id, public_withdrawal_hash, currency_id, destination_address, amount,
  tx_hash, withdrawal_status_id, requested_at, completed_at, failed_at
)
SELECT
  5,
  SHA2('sample-pending-crypto-withdrawal-user-5', 256),
  @btc_id,
  'sample_pending_address_005',
  CAST(1.50000000 AS DECIMAL(36, 18)),
  NULL,
  @withdrawal_pending_id,
  '2026-02-10 10:00:00',
  NULL,
  NULL
FROM DUAL
WHERE NOT EXISTS (
  SELECT 1
  FROM crypto_withdrawals
  WHERE public_withdrawal_hash = SHA2('sample-pending-crypto-withdrawal-user-5', 256)
);

INSERT INTO fiat_deposits (
  user_id, public_deposit_hash, currency_id, amount, deposit_status_id,
  requested_at, completed_at, failed_at
)
SELECT
  1,
  SHA2('sample-monitoring-fiat-deposit-user-1', 256),
  @jpy_id,
  CAST(1000000 AS DECIMAL(36, 18)),
  @deposit_completed_id,
  '2026-02-01 09:00:00',
  '2026-02-01 09:05:00',
  NULL
FROM DUAL
WHERE NOT EXISTS (
  SELECT 1
  FROM fiat_deposits
  WHERE public_deposit_hash = SHA2('sample-monitoring-fiat-deposit-user-1', 256)
);

INSERT INTO fiat_deposits (
  user_id, public_deposit_hash, currency_id, amount, deposit_status_id,
  requested_at, completed_at, failed_at
)
SELECT
  4,
  SHA2('sample-pending-fiat-deposit-user-4', 256),
  @jpy_id,
  CAST(250000 AS DECIMAL(36, 18)),
  @deposit_pending_id,
  '2026-02-10 09:00:00',
  NULL,
  NULL
FROM DUAL
WHERE NOT EXISTS (
  SELECT 1
  FROM fiat_deposits
  WHERE public_deposit_hash = SHA2('sample-pending-fiat-deposit-user-4', 256)
);

INSERT INTO crypto_deposits (
  user_id, public_deposit_hash, currency_id, tx_hash, amount, deposit_status_id,
  detected_at, confirmed_at, failed_at
)
SELECT
  4,
  SHA2('sample-large-unmatched-crypto-deposit-user-4', 256),
  @btc_id,
  SHA2('sample-large-unmatched-crypto-deposit-user-4-tx', 256),
  CAST(2.50000000 AS DECIMAL(36, 18)),
  @deposit_completed_id,
  '2026-02-11 08:00:00',
  '2026-02-11 08:30:00',
  NULL
FROM DUAL
WHERE NOT EXISTS (
  SELECT 1
  FROM crypto_deposits
  WHERE public_deposit_hash = SHA2('sample-large-unmatched-crypto-deposit-user-4', 256)
);

INSERT INTO fiat_withdrawals (
  user_id, public_withdrawal_hash, currency_id, amount, withdrawal_status_id,
  requested_at, completed_at, failed_at
)
SELECT
  1,
  SHA2('sample-monitoring-fiat-withdrawal-user-1', 256),
  @jpy_id,
  CAST(900000 AS DECIMAL(36, 18)),
  @withdrawal_completed_id,
  '2026-02-01 13:00:00',
  '2026-02-01 13:20:00',
  NULL
FROM DUAL
WHERE NOT EXISTS (
  SELECT 1
  FROM fiat_withdrawals
  WHERE public_withdrawal_hash = SHA2('sample-monitoring-fiat-withdrawal-user-1', 256)
);

INSERT INTO alert_rules (
  public_rule_hash, rule_name, rule_type, severity, threshold_json, is_active
)
VALUES
  (
    SHA2('sample-rule-rapid-fiat-outflow', 256),
    'Rapid Fiat Outflow 24h',
    'RAPID_OUTFLOW',
    'CRITICAL',
    JSON_OBJECT('window_hours', 24, 'outflow_ratio', 0.8, 'currency_scope', 'SAME_CURRENCY'),
    1
  ),
  (
    SHA2('sample-rule-high-value-withdrawal', 256),
    'High Value Withdrawal',
    'LARGE_WITHDRAWAL',
    'HIGH',
    JSON_OBJECT('single_withdrawal_amount', 500000, 'currency', 'JPY'),
    1
  )
ON DUPLICATE KEY UPDATE
  public_rule_hash = VALUES(public_rule_hash),
  rule_type = VALUES(rule_type),
  severity = VALUES(severity),
  threshold_json = VALUES(threshold_json),
  is_active = VALUES(is_active),
  updated_at = CURRENT_TIMESTAMP(6);

SET @sample_rapid_outflow_rule_id := (
  SELECT id FROM alert_rules WHERE rule_name = 'Rapid Fiat Outflow 24h' LIMIT 1
);
SET @sample_large_withdrawal_rule_id := (
  SELECT id FROM alert_rules WHERE rule_name = 'High Value Withdrawal' LIMIT 1
);
SET @sample_rapid_outflow_withdrawal_id := (
  SELECT id
  FROM fiat_withdrawals
  WHERE public_withdrawal_hash = SHA2('sample-monitoring-fiat-withdrawal-user-1', 256)
  LIMIT 1
);
SET @sample_trade_execution_id := (
  SELECT id
  FROM trade_executions
  WHERE public_execution_hash = SHA2('sample-exec-1', 256)
  LIMIT 1
);

INSERT INTO alert_event_logs (
  user_id, rule_id, alert_event_status_id, trade_execution_id, fiat_deposit_id,
  fiat_withdrawal_id, crypto_deposit_id, crypto_withdrawal_id, score, detected_at, note
)
SELECT
  1,
  @sample_rapid_outflow_rule_id,
  @alert_status_open_id,
  NULL,
  NULL,
  @sample_rapid_outflow_withdrawal_id,
  NULL,
  NULL,
  98.5000,
  '2026-02-01 13:25:00',
  'sample-alert-rapid-outflow-user-1'
FROM DUAL
WHERE NOT EXISTS (
  SELECT 1
  FROM alert_event_logs
  WHERE note = 'sample-alert-rapid-outflow-user-1'
);

INSERT INTO alert_event_logs (
  user_id, rule_id, alert_event_status_id, trade_execution_id, fiat_deposit_id,
  fiat_withdrawal_id, crypto_deposit_id, crypto_withdrawal_id, score, detected_at, note
)
SELECT
  2,
  @sample_large_withdrawal_rule_id,
  @alert_status_reviewed_id,
  @sample_trade_execution_id,
  NULL,
  NULL,
  NULL,
  NULL,
  72.2500,
  '2026-02-02 10:00:00',
  'sample-alert-reviewed-trade-user-2'
FROM DUAL
WHERE NOT EXISTS (
  SELECT 1
  FROM alert_event_logs
  WHERE note = 'sample-alert-reviewed-trade-user-2'
);

INSERT INTO fiat_withdrawals (
  user_id, public_withdrawal_hash, currency_id, amount, withdrawal_status_id,
  requested_at, completed_at, failed_at
)
SELECT
  1,
  SHA2('sample-repeat-alert-fiat-withdrawal-user-1', 256),
  @jpy_id,
  CAST(650000 AS DECIMAL(36, 18)),
  @withdrawal_completed_id,
  '2026-02-01 18:00:00',
  '2026-02-01 18:15:00',
  NULL
FROM DUAL
WHERE NOT EXISTS (
  SELECT 1
  FROM fiat_withdrawals
  WHERE public_withdrawal_hash = SHA2('sample-repeat-alert-fiat-withdrawal-user-1', 256)
);

SET @sample_repeat_alert_withdrawal_id := (
  SELECT id
  FROM fiat_withdrawals
  WHERE public_withdrawal_hash = SHA2('sample-repeat-alert-fiat-withdrawal-user-1', 256)
  LIMIT 1
);

INSERT INTO alert_event_logs (
  user_id, rule_id, alert_event_status_id, trade_execution_id, fiat_deposit_id,
  fiat_withdrawal_id, crypto_deposit_id, crypto_withdrawal_id, score, detected_at, note
)
SELECT
  1,
  @sample_large_withdrawal_rule_id,
  @alert_status_open_id,
  NULL,
  NULL,
  @sample_repeat_alert_withdrawal_id,
  NULL,
  NULL,
  81.0000,
  '2026-02-01 18:20:00',
  'sample-alert-repeat-user-1'
FROM DUAL
WHERE NOT EXISTS (
  SELECT 1
  FROM alert_event_logs
  WHERE note = 'sample-alert-repeat-user-1'
);

SET @sample_rapid_outflow_alert_id := (
  SELECT id
  FROM alert_event_logs
  WHERE note = 'sample-alert-rapid-outflow-user-1'
  LIMIT 1
);

INSERT INTO suspicious_cases (
  user_id, public_case_hash, opened_by_type_id, opened_by_id, source_type_id,
  alert_event_log_id, title, current_status_id, risk_level_id, assigned_to, opened_at, closed_at, closed_reason, disposition
)
SELECT
  1,
  SHA2('sample-case-rapid-outflow-user-1', 256),
  @actor_system_id,
  'monitoring-batch',
  @case_source_auto_id,
  @sample_rapid_outflow_alert_id,
  'Rapid outflow after fiat deposit',
  @case_status_investigating_id,
  @risk_critical_id,
  'aml-operator-001',
  '2026-02-01 13:30:00',
  NULL,
  NULL,
  NULL
FROM DUAL
WHERE NOT EXISTS (
  SELECT 1
  FROM suspicious_cases
  WHERE public_case_hash = SHA2('sample-case-rapid-outflow-user-1', 256)
);

INSERT INTO suspicious_cases (
  user_id, public_case_hash, opened_by_type_id, opened_by_id, source_type_id,
  alert_event_log_id, title, current_status_id, risk_level_id, assigned_to, opened_at, closed_at, closed_reason, disposition
)
SELECT
  3,
  SHA2('sample-case-manual-review-user-3', 256),
  @actor_system_id,
  'aml-console',
  @case_source_auto_id,
  NULL,
  'Manual review queue seed case',
  @case_status_open_id,
  @risk_high_id,
  'aml-operator-002',
  '2026-02-02 09:00:00',
  NULL,
  NULL,
  NULL
FROM DUAL
WHERE NOT EXISTS (
  SELECT 1
  FROM suspicious_cases
  WHERE public_case_hash = SHA2('sample-case-manual-review-user-3', 256)
);

INSERT INTO user_status_change_events (
  user_id, event_type_id, actor_type_id, actor_id, reason, occurred_at
)
SELECT
  1,
  @frozen_event_type_id,
  @actor_system_id,
  'monitoring-batch',
  'rapid outflow escalation',
  '2026-02-01 13:26:00'
FROM DUAL
WHERE NOT EXISTS (
  SELECT 1
  FROM user_status_change_events
  WHERE actor_id = 'monitoring-batch'
    AND reason = 'rapid outflow escalation'
    AND occurred_at = '2026-02-01 13:26:00'
);

SET @sample_status_change_event_id := (
  SELECT id
  FROM user_status_change_events
  WHERE actor_id = 'monitoring-batch'
    AND reason = 'rapid outflow escalation'
    AND occurred_at = '2026-02-01 13:26:00'
  LIMIT 1
);

INSERT INTO user_status_histories (
  user_id, status_change_event_id, from_status_id, to_status_id, changed_at
)
SELECT
  1,
  @sample_status_change_event_id,
  @status_active_id,
  @status_frozen_id,
  '2026-02-01 13:27:00'
FROM DUAL
WHERE NOT EXISTS (
  SELECT 1
  FROM user_status_histories
  WHERE status_change_event_id = @sample_status_change_event_id
);

INSERT INTO account_actions (
  user_id, suspicious_case_id, action_type_id, actor_type_id, actor_id, action_reason, requested_at, completed_at
)
SELECT
  1,
  (SELECT id FROM suspicious_cases WHERE public_case_hash = SHA2('sample-case-rapid-outflow-user-1', 256) LIMIT 1),
  @freeze_action_type_id,
  @actor_system_id,
  'monitoring-batch',
  'auto freeze after rapid outflow detection',
  '2026-02-01 13:28:00',
  '2026-02-01 13:35:00'
FROM DUAL
WHERE NOT EXISTS (
  SELECT 1
  FROM account_actions
  WHERE actor_id = 'monitoring-batch'
    AND action_reason = 'auto freeze after rapid outflow detection'
    AND requested_at = '2026-02-01 13:28:00'
);

INSERT INTO suspicious_cases (
  user_id, public_case_hash, opened_by_type_id, opened_by_id, source_type_id,
  alert_event_log_id, title, current_status_id, risk_level_id, assigned_to, opened_at, closed_at, closed_reason, disposition
)
SELECT
  2,
  SHA2('sample-case-closed-user-2', 256),
  @actor_system_id,
  'aml-console',
  @case_source_auto_id,
  (SELECT id FROM alert_event_logs WHERE note = 'sample-alert-reviewed-trade-user-2' LIMIT 1),
  'Closed review sample case',
  @case_status_closed_id,
  @risk_high_id,
  'aml-operator-003',
  '2026-02-02 10:05:00',
  '2026-02-04 10:05:00',
  'false positive confirmed',
  'FALSE_POSITIVE'
FROM DUAL
WHERE NOT EXISTS (
  SELECT 1
  FROM suspicious_cases
  WHERE public_case_hash = SHA2('sample-case-closed-user-2', 256)
);

COMMIT;
