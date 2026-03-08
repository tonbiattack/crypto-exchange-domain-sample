CREATE DATABASE IF NOT EXISTS exchange_domain CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE exchange_domain;

SET NAMES utf8mb4;
SET time_zone = '+09:00';

-- master tables
CREATE TABLE IF NOT EXISTS actor_types (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '実行者種別ID',
  name VARCHAR(64) NOT NULL COMMENT '表示名',
  value VARCHAR(64) NOT NULL COMMENT 'システム識別値',
  description VARCHAR(255) NOT NULL COMMENT '説明',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時',
  UNIQUE KEY uk_actor_types_value (value)
) ENGINE=InnoDB COMMENT='実行者種別マスタ';

CREATE TABLE IF NOT EXISTS user_statuses (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT 'ユーザーステータスID',
  name VARCHAR(64) NOT NULL COMMENT '表示名',
  value VARCHAR(64) NOT NULL COMMENT 'システム識別値',
  description VARCHAR(255) NOT NULL COMMENT '説明',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時',
  UNIQUE KEY uk_user_statuses_value (value)
) ENGINE=InnoDB COMMENT='ユーザーステータス区分マスタ';

CREATE TABLE IF NOT EXISTS order_statuses (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '注文ステータスID',
  name VARCHAR(64) NOT NULL COMMENT '表示名',
  value VARCHAR(64) NOT NULL COMMENT 'システム識別値',
  description VARCHAR(255) NOT NULL COMMENT '説明',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時',
  UNIQUE KEY uk_order_statuses_value (value)
) ENGINE=InnoDB COMMENT='注文ステータス区分マスタ';

CREATE TABLE IF NOT EXISTS deposit_statuses (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '入金ステータスID',
  name VARCHAR(64) NOT NULL COMMENT '表示名',
  value VARCHAR(64) NOT NULL COMMENT 'システム識別値',
  description VARCHAR(255) NOT NULL COMMENT '説明',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時',
  UNIQUE KEY uk_deposit_statuses_value (value)
) ENGINE=InnoDB COMMENT='入金ステータス区分マスタ';

CREATE TABLE IF NOT EXISTS withdrawal_statuses (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '出金ステータスID',
  name VARCHAR(64) NOT NULL COMMENT '表示名',
  value VARCHAR(64) NOT NULL COMMENT 'システム識別値',
  description VARCHAR(255) NOT NULL COMMENT '説明',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時',
  UNIQUE KEY uk_withdrawal_statuses_value (value)
) ENGINE=InnoDB COMMENT='出金ステータス区分マスタ';

CREATE TABLE IF NOT EXISTS alert_event_statuses (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '検知イベントステータスID',
  name VARCHAR(64) NOT NULL COMMENT '表示名',
  value VARCHAR(64) NOT NULL COMMENT 'システム識別値',
  description VARCHAR(255) NOT NULL COMMENT '説明',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時',
  UNIQUE KEY uk_alert_event_statuses_value (value)
) ENGINE=InnoDB COMMENT='検知イベントステータス区分マスタ';

CREATE TABLE IF NOT EXISTS case_source_types (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT 'ケース起票元種別ID',
  name VARCHAR(64) NOT NULL COMMENT '表示名',
  value VARCHAR(64) NOT NULL COMMENT 'システム識別値',
  description VARCHAR(255) NOT NULL COMMENT '説明',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時',
  UNIQUE KEY uk_case_source_types_value (value)
) ENGINE=InnoDB COMMENT='ケース起票元種別マスタ';

CREATE TABLE IF NOT EXISTS case_statuses (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT 'ケースステータスID',
  name VARCHAR(64) NOT NULL COMMENT '表示名',
  value VARCHAR(64) NOT NULL COMMENT 'システム識別値',
  description VARCHAR(255) NOT NULL COMMENT '説明',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時',
  UNIQUE KEY uk_case_statuses_value (value)
) ENGINE=InnoDB COMMENT='ケースステータス区分マスタ';
CREATE TABLE IF NOT EXISTS risk_levels (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT 'リスクレベルID',
  name VARCHAR(64) NOT NULL COMMENT '表示名',
  value VARCHAR(64) NOT NULL COMMENT 'システム識別値',
  description VARCHAR(255) NOT NULL COMMENT '説明',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時',
  UNIQUE KEY uk_risk_levels_value (value)
) ENGINE=InnoDB COMMENT='ケースリスクレベルマスタ';

CREATE TABLE IF NOT EXISTS account_action_types (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '口座措置種別ID',
  name VARCHAR(64) NOT NULL COMMENT '表示名',
  value VARCHAR(64) NOT NULL COMMENT 'システム識別値',
  description VARCHAR(255) NOT NULL COMMENT '説明',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時',
  UNIQUE KEY uk_account_action_types_value (value)
) ENGINE=InnoDB COMMENT='口座措置種別マスタ';

CREATE TABLE IF NOT EXISTS user_status_event_types (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT 'ユーザーステータス変更イベント種別ID',
  name VARCHAR(64) NOT NULL COMMENT '表示名',
  value VARCHAR(64) NOT NULL COMMENT 'システム識別値',
  description VARCHAR(255) NOT NULL COMMENT '説明',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時',
  UNIQUE KEY uk_user_status_event_types_value (value)
) ENGINE=InnoDB COMMENT='ユーザーステータス変更イベント種別マスタ';

CREATE TABLE IF NOT EXISTS occupations (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '職業ID',
  name VARCHAR(128) NOT NULL COMMENT '表示名',
  value VARCHAR(64) NOT NULL COMMENT 'システム識別値',
  description VARCHAR(255) NOT NULL COMMENT '説明',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時',
  UNIQUE KEY uk_occupations_value (value)
) ENGINE=InnoDB COMMENT='職業区分マスタ';

-- annual_income_brackets は user_profile_versions.annual_income_bracket_id の参照先。
CREATE TABLE IF NOT EXISTS annual_income_brackets (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '年収区分ID',
  name VARCHAR(128) NOT NULL COMMENT '表示名',
  value VARCHAR(64) NOT NULL COMMENT 'システム識別値',
  description VARCHAR(255) NOT NULL COMMENT '説明',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時',
  UNIQUE KEY uk_annual_income_brackets_value (value)
) ENGINE=InnoDB COMMENT='年収区分マスタ';

-- financial_asset_brackets は user_profile_versions.financial_asset_bracket_id の参照先。
CREATE TABLE IF NOT EXISTS financial_asset_brackets (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '金融資産区分ID',
  name VARCHAR(128) NOT NULL COMMENT '表示名',
  value VARCHAR(64) NOT NULL COMMENT 'システム識別値',
  description VARCHAR(255) NOT NULL COMMENT '説明',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時',
  UNIQUE KEY uk_financial_asset_brackets_value (value)
) ENGINE=InnoDB COMMENT='金融資産区分マスタ';

-- currencies は取引・入出金・手数料通貨の参照先。
CREATE TABLE IF NOT EXISTS currencies (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '通貨ID',
  code VARCHAR(16) NOT NULL COMMENT '通貨コード',
  name VARCHAR(64) NOT NULL COMMENT '通貨名',
  currency_type VARCHAR(16) NOT NULL COMMENT '通貨種別(FIAT/CRYPTO)',
  description VARCHAR(255) NOT NULL COMMENT '説明',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時',
  UNIQUE KEY uk_currencies_code (code)
) ENGINE=InnoDB COMMENT='通貨マスタ';
-- users は現在値のみ。状態変更の時系列は user_status_change_events / user_status_histories で管理する。
CREATE TABLE IF NOT EXISTS users (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT 'ユーザーID',
  public_user_hash CHAR(64) NOT NULL COMMENT '外部公開用ハッシュID',
  member_code VARCHAR(32) NOT NULL COMMENT '会員コード',
  current_status_id BIGINT UNSIGNED NOT NULL COMMENT '現在ステータスID',
  registered_at DATETIME(6) NOT NULL COMMENT '本登録日時',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時',
  UNIQUE KEY uk_users_public_user_hash (public_user_hash),
  UNIQUE KEY uk_users_member_code (member_code),
  KEY idx_users_current_status_id (current_status_id),
  CONSTRAINT fk_users_current_status FOREIGN KEY (current_status_id) REFERENCES user_statuses(id)
) ENGINE=InnoDB COMMENT='ユーザー最新状態テーブル(現在値のみ)';

CREATE TABLE IF NOT EXISTS provisional_registrations (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '仮登録ID',
  public_registration_hash CHAR(64) NOT NULL COMMENT '外部公開用仮登録ハッシュID',
  email VARCHAR(320) NOT NULL COMMENT 'メールアドレス',
  token_hash CHAR(64) NOT NULL COMMENT '認証トークンハッシュ',
  expires_at DATETIME(6) NOT NULL COMMENT '有効期限',
  verified_at DATETIME(6) NULL COMMENT '認証完了日時',
  cancelled_at DATETIME(6) NULL COMMENT 'キャンセル日時',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時'
) ENGINE=InnoDB COMMENT='仮登録テーブル';

CREATE TABLE IF NOT EXISTS user_profile_versions (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT 'プロファイル履歴ID',
  user_id BIGINT UNSIGNED NOT NULL COMMENT 'ユーザーID',
  version_no INT UNSIGNED NOT NULL COMMENT '版番号',
  last_name VARCHAR(100) NOT NULL COMMENT '姓',
  first_name VARCHAR(100) NOT NULL COMMENT '名',
  birth_date DATE NOT NULL COMMENT '生年月日',
  country_code CHAR(2) NOT NULL COMMENT '国コード',
  occupation_id BIGINT UNSIGNED NOT NULL COMMENT '職業区分ID',
  annual_income_bracket_id BIGINT UNSIGNED NOT NULL COMMENT '年収区分ID',
  financial_asset_bracket_id BIGINT UNSIGNED NOT NULL COMMENT '金融資産区分ID',
  declared_at DATETIME(6) NOT NULL COMMENT '申告日時',
  change_reason VARCHAR(255) NOT NULL COMMENT '変更理由',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  UNIQUE KEY uk_user_profile_versions_user_version (user_id, version_no),
  CONSTRAINT fk_user_profile_versions_user FOREIGN KEY (user_id) REFERENCES users(id),
  CONSTRAINT fk_user_profile_versions_occupation FOREIGN KEY (occupation_id) REFERENCES occupations(id),
  CONSTRAINT fk_user_profile_versions_income FOREIGN KEY (annual_income_bracket_id) REFERENCES annual_income_brackets(id),
  CONSTRAINT fk_user_profile_versions_assets FOREIGN KEY (financial_asset_bracket_id) REFERENCES financial_asset_brackets(id)
) ENGINE=InnoDB COMMENT='ユーザープロファイル履歴テーブル(追記専用)';

CREATE TABLE IF NOT EXISTS user_status_change_events (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT 'ステータス変更イベントID',
  user_id BIGINT UNSIGNED NOT NULL COMMENT 'ユーザーID',
  event_type_id BIGINT UNSIGNED NOT NULL COMMENT 'イベント種別ID',
  actor_type_id BIGINT UNSIGNED NOT NULL COMMENT '実行者種別ID',
  actor_id VARCHAR(64) NOT NULL COMMENT '実行者識別子',
  reason VARCHAR(255) NOT NULL COMMENT '変更理由',
  occurred_at DATETIME(6) NOT NULL COMMENT 'イベント発生日時',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  CONSTRAINT fk_user_status_change_events_user FOREIGN KEY (user_id) REFERENCES users(id),
  CONSTRAINT fk_user_status_change_events_type FOREIGN KEY (event_type_id) REFERENCES user_status_event_types(id),
  CONSTRAINT fk_user_status_change_events_actor_type FOREIGN KEY (actor_type_id) REFERENCES actor_types(id)
) ENGINE=InnoDB COMMENT='ユーザーステータス変更イベント(追記専用)';

CREATE TABLE IF NOT EXISTS user_status_histories (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT 'ユーザーステータス履歴ID',
  user_id BIGINT UNSIGNED NOT NULL COMMENT 'ユーザーID',
  status_change_event_id BIGINT UNSIGNED NOT NULL COMMENT 'イベントID',
  from_status_id BIGINT UNSIGNED NULL COMMENT '変更前ステータスID',
  to_status_id BIGINT UNSIGNED NOT NULL COMMENT '変更後ステータスID',
  changed_at DATETIME(6) NOT NULL COMMENT '変更日時',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  CONSTRAINT fk_user_status_histories_user FOREIGN KEY (user_id) REFERENCES users(id),
  CONSTRAINT fk_user_status_histories_event FOREIGN KEY (status_change_event_id) REFERENCES user_status_change_events(id),
  CONSTRAINT fk_user_status_histories_from_status FOREIGN KEY (from_status_id) REFERENCES user_statuses(id),
  CONSTRAINT fk_user_status_histories_to_status FOREIGN KEY (to_status_id) REFERENCES user_statuses(id)
) ENGINE=InnoDB COMMENT='ユーザーステータス変更履歴(追記専用)';
CREATE TABLE IF NOT EXISTS trading_orders (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '注文ID',
  user_id BIGINT UNSIGNED NOT NULL COMMENT 'ユーザーID',
  public_order_hash CHAR(64) NOT NULL COMMENT '外部公開用注文ハッシュID',
  side VARCHAR(8) NOT NULL COMMENT '売買区分(BUY/SELL)',
  order_type VARCHAR(16) NOT NULL COMMENT '注文種別(LIMIT/MARKET)',
  from_currency_id BIGINT UNSIGNED NOT NULL COMMENT '交換元通貨ID',
  to_currency_id BIGINT UNSIGNED NOT NULL COMMENT '交換先通貨ID',
  price DECIMAL(36, 18) NOT NULL COMMENT '注文価格',
  quantity DECIMAL(36, 18) NOT NULL COMMENT '注文数量',
  order_status_id BIGINT UNSIGNED NOT NULL COMMENT '注文ステータスID',
  placed_at DATETIME(6) NOT NULL COMMENT '注文受付日時',
  cancelled_at DATETIME(6) NULL COMMENT '注文キャンセル日時',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時',
  UNIQUE KEY uk_trading_orders_public_hash (public_order_hash),
  KEY idx_trading_orders_user_status_placed (user_id, order_status_id, placed_at),
  CONSTRAINT fk_trading_orders_user FOREIGN KEY (user_id) REFERENCES users(id),
  CONSTRAINT fk_trading_orders_order_status FOREIGN KEY (order_status_id) REFERENCES order_statuses(id),
  CONSTRAINT fk_trading_orders_from_currency FOREIGN KEY (from_currency_id) REFERENCES currencies(id),
  CONSTRAINT fk_trading_orders_to_currency FOREIGN KEY (to_currency_id) REFERENCES currencies(id)
) ENGINE=InnoDB COMMENT='取引注文テーブル';

CREATE TABLE IF NOT EXISTS trade_executions (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '約定ID',
  order_id BIGINT UNSIGNED NOT NULL COMMENT '注文ID',
  user_id BIGINT UNSIGNED NOT NULL COMMENT 'ユーザーID',
  public_execution_hash CHAR(64) NOT NULL COMMENT '外部公開用約定ハッシュID',
  from_currency_id BIGINT UNSIGNED NOT NULL COMMENT '交換元通貨ID',
  to_currency_id BIGINT UNSIGNED NOT NULL COMMENT '交換先通貨ID',
  executed_price DECIMAL(36, 18) NOT NULL COMMENT '約定価格',
  executed_quantity DECIMAL(36, 18) NOT NULL COMMENT '約定数量',
  fee_currency_id BIGINT UNSIGNED NOT NULL COMMENT '手数料通貨ID',
  fee_amount DECIMAL(36, 18) NOT NULL DEFAULT 0 COMMENT '手数料数量',
  executed_at DATETIME(6) NOT NULL COMMENT '約定日時',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  UNIQUE KEY uk_trade_executions_public_hash (public_execution_hash),
  CONSTRAINT fk_trade_executions_order FOREIGN KEY (order_id) REFERENCES trading_orders(id),
  CONSTRAINT fk_trade_executions_user FOREIGN KEY (user_id) REFERENCES users(id),
  CONSTRAINT fk_trade_executions_from_currency FOREIGN KEY (from_currency_id) REFERENCES currencies(id),
  CONSTRAINT fk_trade_executions_to_currency FOREIGN KEY (to_currency_id) REFERENCES currencies(id),
  CONSTRAINT fk_trade_executions_fee_currency FOREIGN KEY (fee_currency_id) REFERENCES currencies(id)
) ENGINE=InnoDB COMMENT='約定テーブル(追記専用)';

CREATE TABLE IF NOT EXISTS fiat_deposits (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '法定入金ID',
  user_id BIGINT UNSIGNED NOT NULL COMMENT 'ユーザーID',
  public_deposit_hash CHAR(64) NOT NULL COMMENT '外部公開用入金ハッシュID',
  currency_id BIGINT UNSIGNED NOT NULL COMMENT '通貨ID',
  amount DECIMAL(36, 18) NOT NULL COMMENT '入金額',
  deposit_status_id BIGINT UNSIGNED NOT NULL COMMENT '入金ステータスID',
  requested_at DATETIME(6) NOT NULL COMMENT '入金申請日時',
  completed_at DATETIME(6) NULL COMMENT '入金完了日時',
  failed_at DATETIME(6) NULL COMMENT '入金失敗日時',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時',
  CONSTRAINT fk_fiat_deposits_user FOREIGN KEY (user_id) REFERENCES users(id),
  CONSTRAINT fk_fiat_deposits_currency FOREIGN KEY (currency_id) REFERENCES currencies(id),
  CONSTRAINT fk_fiat_deposits_status FOREIGN KEY (deposit_status_id) REFERENCES deposit_statuses(id)
) ENGINE=InnoDB COMMENT='法定通貨入金テーブル';

CREATE TABLE IF NOT EXISTS fiat_withdrawals (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '法定出金ID',
  user_id BIGINT UNSIGNED NOT NULL COMMENT 'ユーザーID',
  public_withdrawal_hash CHAR(64) NOT NULL COMMENT '外部公開用出金ハッシュID',
  currency_id BIGINT UNSIGNED NOT NULL COMMENT '通貨ID',
  amount DECIMAL(36, 18) NOT NULL COMMENT '出金額',
  withdrawal_status_id BIGINT UNSIGNED NOT NULL COMMENT '出金ステータスID',
  requested_at DATETIME(6) NOT NULL COMMENT '出金申請日時',
  completed_at DATETIME(6) NULL COMMENT '出金完了日時',
  failed_at DATETIME(6) NULL COMMENT '出金失敗日時',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時',
  CONSTRAINT fk_fiat_withdrawals_user FOREIGN KEY (user_id) REFERENCES users(id),
  CONSTRAINT fk_fiat_withdrawals_currency FOREIGN KEY (currency_id) REFERENCES currencies(id),
  CONSTRAINT fk_fiat_withdrawals_status FOREIGN KEY (withdrawal_status_id) REFERENCES withdrawal_statuses(id)
) ENGINE=InnoDB COMMENT='法定通貨出金テーブル';
CREATE TABLE IF NOT EXISTS crypto_deposits (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '暗号資産入金ID',
  user_id BIGINT UNSIGNED NOT NULL COMMENT 'ユーザーID',
  public_deposit_hash CHAR(64) NOT NULL COMMENT '外部公開用入金ハッシュID',
  currency_id BIGINT UNSIGNED NOT NULL COMMENT '通貨ID',
  tx_hash VARCHAR(128) NOT NULL COMMENT 'トランザクションハッシュ',
  amount DECIMAL(36, 18) NOT NULL COMMENT '入金数量',
  deposit_status_id BIGINT UNSIGNED NOT NULL COMMENT '入金ステータスID',
  detected_at DATETIME(6) NOT NULL COMMENT '検知日時',
  confirmed_at DATETIME(6) NULL COMMENT '承認完了日時',
  failed_at DATETIME(6) NULL COMMENT '入金失敗日時',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時',
  CONSTRAINT fk_crypto_deposits_user FOREIGN KEY (user_id) REFERENCES users(id),
  CONSTRAINT fk_crypto_deposits_currency FOREIGN KEY (currency_id) REFERENCES currencies(id),
  CONSTRAINT fk_crypto_deposits_status FOREIGN KEY (deposit_status_id) REFERENCES deposit_statuses(id)
) ENGINE=InnoDB COMMENT='暗号資産入金テーブル';

CREATE TABLE IF NOT EXISTS crypto_withdrawals (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '暗号資産出金ID',
  user_id BIGINT UNSIGNED NOT NULL COMMENT 'ユーザーID',
  public_withdrawal_hash CHAR(64) NOT NULL COMMENT '外部公開用出金ハッシュID',
  currency_id BIGINT UNSIGNED NOT NULL COMMENT '通貨ID',
  destination_address VARCHAR(255) NOT NULL COMMENT '送金先アドレス',
  amount DECIMAL(36, 18) NOT NULL COMMENT '出金数量',
  tx_hash VARCHAR(128) NULL COMMENT 'トランザクションハッシュ',
  withdrawal_status_id BIGINT UNSIGNED NOT NULL COMMENT '出金ステータスID',
  requested_at DATETIME(6) NOT NULL COMMENT '出金申請日時',
  completed_at DATETIME(6) NULL COMMENT '出金完了日時',
  failed_at DATETIME(6) NULL COMMENT '出金失敗日時',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時',
  CONSTRAINT fk_crypto_withdrawals_user FOREIGN KEY (user_id) REFERENCES users(id),
  CONSTRAINT fk_crypto_withdrawals_currency FOREIGN KEY (currency_id) REFERENCES currencies(id),
  CONSTRAINT fk_crypto_withdrawals_status FOREIGN KEY (withdrawal_status_id) REFERENCES withdrawal_statuses(id)
) ENGINE=InnoDB COMMENT='暗号資産出金テーブル';

CREATE TABLE IF NOT EXISTS alert_rules (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT 'ルールID',
  public_rule_hash CHAR(64) NOT NULL COMMENT '外部公開用ルールハッシュID',
  rule_name VARCHAR(128) NOT NULL COMMENT 'ルール名',
  rule_type VARCHAR(32) NOT NULL COMMENT 'ルール種別',
  severity VARCHAR(16) NOT NULL COMMENT '重要度',
  threshold_json JSON NOT NULL COMMENT '閾値設定JSON',
  is_active TINYINT(1) NOT NULL DEFAULT 1 COMMENT '有効フラグ',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時',
  UNIQUE KEY uk_alert_rules_name (rule_name)
) ENGINE=InnoDB COMMENT='自動検知ルールテーブル';

CREATE TABLE IF NOT EXISTS alert_event_logs (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '検知イベントID',
  user_id BIGINT UNSIGNED NOT NULL COMMENT 'ユーザーID',
  rule_id BIGINT UNSIGNED NOT NULL COMMENT 'ルールID',
  alert_event_status_id BIGINT UNSIGNED NOT NULL COMMENT '検知イベントステータスID',
  trade_execution_id BIGINT UNSIGNED NULL COMMENT '対象約定ID',
  fiat_deposit_id BIGINT UNSIGNED NULL COMMENT '対象法定入金ID',
  fiat_withdrawal_id BIGINT UNSIGNED NULL COMMENT '対象法定出金ID',
  crypto_deposit_id BIGINT UNSIGNED NULL COMMENT '対象暗号資産入金ID',
  crypto_withdrawal_id BIGINT UNSIGNED NULL COMMENT '対象暗号資産出金ID',
  score DECIMAL(10, 4) NOT NULL COMMENT '検知スコア',
  detected_at DATETIME(6) NOT NULL COMMENT '検知日時',
  note VARCHAR(255) NOT NULL COMMENT 'メモ',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  CONSTRAINT fk_alert_event_logs_user FOREIGN KEY (user_id) REFERENCES users(id),
  CONSTRAINT fk_alert_event_logs_rule FOREIGN KEY (rule_id) REFERENCES alert_rules(id),
  CONSTRAINT fk_alert_event_logs_status FOREIGN KEY (alert_event_status_id) REFERENCES alert_event_statuses(id),
  CONSTRAINT chk_alert_event_logs_target CHECK ((trade_execution_id IS NOT NULL) + (fiat_deposit_id IS NOT NULL) + (fiat_withdrawal_id IS NOT NULL) + (crypto_deposit_id IS NOT NULL) + (crypto_withdrawal_id IS NOT NULL) = 1)
) ENGINE=InnoDB COMMENT='自動検知イベント履歴テーブル(追記専用)';

CREATE TABLE IF NOT EXISTS suspicious_cases (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT 'ケースID',
  user_id BIGINT UNSIGNED NOT NULL COMMENT 'ユーザーID',
  public_case_hash CHAR(64) NOT NULL COMMENT '外部公開用ケースハッシュID',
  opened_by_type_id BIGINT UNSIGNED NOT NULL COMMENT '起票者種別ID',
  opened_by_id VARCHAR(64) NOT NULL COMMENT '起票者識別子',
  source_type_id BIGINT UNSIGNED NOT NULL COMMENT '起票元種別ID',
  alert_event_log_id BIGINT UNSIGNED NULL COMMENT '起点検知イベントID',
  title VARCHAR(255) NOT NULL COMMENT 'ケースタイトル',
  current_status_id BIGINT UNSIGNED NOT NULL COMMENT '現在ケースステータスID',
  risk_level_id BIGINT UNSIGNED NOT NULL COMMENT 'リスクレベルID',
  opened_at DATETIME(6) NOT NULL COMMENT '開始日時',
  closed_at DATETIME(6) NULL COMMENT '終了日時',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時',
  CONSTRAINT fk_suspicious_cases_user FOREIGN KEY (user_id) REFERENCES users(id),
  CONSTRAINT fk_suspicious_cases_opened_type FOREIGN KEY (opened_by_type_id) REFERENCES actor_types(id),
  CONSTRAINT fk_suspicious_cases_source FOREIGN KEY (source_type_id) REFERENCES case_source_types(id),
  CONSTRAINT fk_suspicious_cases_status FOREIGN KEY (current_status_id) REFERENCES case_statuses(id),
  CONSTRAINT fk_suspicious_cases_risk FOREIGN KEY (risk_level_id) REFERENCES risk_levels(id)
) ENGINE=InnoDB COMMENT='疑わしい取引ケース管理テーブル';

CREATE TABLE IF NOT EXISTS case_status_histories (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT 'ケース履歴ID',
  case_id BIGINT UNSIGNED NOT NULL COMMENT 'ケースID',
  from_status_id BIGINT UNSIGNED NULL COMMENT '変更前ステータスID',
  to_status_id BIGINT UNSIGNED NOT NULL COMMENT '変更後ステータスID',
  actor_type_id BIGINT UNSIGNED NOT NULL COMMENT '実行者種別ID',
  actor_id VARCHAR(64) NOT NULL COMMENT '実行者識別子',
  reason VARCHAR(255) NOT NULL COMMENT '変更理由',
  changed_at DATETIME(6) NOT NULL COMMENT '変更日時',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  CONSTRAINT fk_case_status_histories_case FOREIGN KEY (case_id) REFERENCES suspicious_cases(id),
  CONSTRAINT fk_case_status_histories_from FOREIGN KEY (from_status_id) REFERENCES case_statuses(id),
  CONSTRAINT fk_case_status_histories_to FOREIGN KEY (to_status_id) REFERENCES case_statuses(id),
  CONSTRAINT fk_case_status_histories_actor_type FOREIGN KEY (actor_type_id) REFERENCES actor_types(id)
) ENGINE=InnoDB COMMENT='ケースステータス変更履歴(追記専用)';

CREATE TABLE IF NOT EXISTS account_actions (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '口座措置ID',
  user_id BIGINT UNSIGNED NOT NULL COMMENT 'ユーザーID',
  suspicious_case_id BIGINT UNSIGNED NULL COMMENT '関連ケースID',
  action_type_id BIGINT UNSIGNED NOT NULL COMMENT '措置種別ID',
  actor_type_id BIGINT UNSIGNED NOT NULL COMMENT '実行者種別ID',
  actor_id VARCHAR(64) NOT NULL COMMENT '実行者識別子',
  action_reason VARCHAR(255) NOT NULL COMMENT '措置理由',
  requested_at DATETIME(6) NOT NULL COMMENT '依頼日時',
  completed_at DATETIME(6) NULL COMMENT '完了日時',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',
  CONSTRAINT fk_account_actions_user FOREIGN KEY (user_id) REFERENCES users(id),
  CONSTRAINT fk_account_actions_action_type FOREIGN KEY (action_type_id) REFERENCES account_action_types(id),
  CONSTRAINT fk_account_actions_actor_type FOREIGN KEY (actor_type_id) REFERENCES actor_types(id)
) ENGINE=InnoDB COMMENT='口座措置履歴(追記専用)';

INSERT INTO user_statuses (name, value, description) VALUES
  ('仮登録', 'PROVISIONAL', '仮登録状態'),
  ('アクティブ', 'ACTIVE', '通常利用可能'),
  ('凍結', 'FROZEN', '一時凍結'),
  ('退会', 'WITHDRAWN', '退会状態')
ON DUPLICATE KEY UPDATE name = VALUES(name), description = VALUES(description), updated_at = CURRENT_TIMESTAMP(6);

INSERT INTO order_statuses (name, value, description) VALUES
  ('受付中', 'OPEN', '注文受付済み'),
  ('約定済み', 'FILLED', '約定完了'),
  ('取消済み', 'CANCELLED', '取消完了')
ON DUPLICATE KEY UPDATE name = VALUES(name), description = VALUES(description), updated_at = CURRENT_TIMESTAMP(6);

INSERT INTO deposit_statuses (name, value, description) VALUES
  ('処理中', 'PENDING', '処理中'),
  ('完了', 'COMPLETED', '処理完了'),
  ('失敗', 'FAILED', '処理失敗')
ON DUPLICATE KEY UPDATE name = VALUES(name), description = VALUES(description), updated_at = CURRENT_TIMESTAMP(6);

INSERT INTO withdrawal_statuses (name, value, description) VALUES
  ('処理中', 'PENDING', '処理中'),
  ('完了', 'COMPLETED', '処理完了'),
  ('失敗', 'FAILED', '処理失敗')
ON DUPLICATE KEY UPDATE name = VALUES(name), description = VALUES(description), updated_at = CURRENT_TIMESTAMP(6);
INSERT INTO actor_types (name, value, description) VALUES
  ('システム', 'SYSTEM', 'システム実行'),
  ('管理者', 'ADMIN', '管理者実行'),
  ('ユーザー', 'USER', 'ユーザー実行')
ON DUPLICATE KEY UPDATE name = VALUES(name), description = VALUES(description), updated_at = CURRENT_TIMESTAMP(6);

INSERT INTO alert_event_statuses (name, value, description) VALUES
  ('新規', 'OPEN', '未対応'),
  ('確認済み', 'REVIEWED', '確認済み'),
  ('無視', 'IGNORED', '許容として無視')
ON DUPLICATE KEY UPDATE name = VALUES(name), description = VALUES(description), updated_at = CURRENT_TIMESTAMP(6);

INSERT INTO case_source_types (name, value, description) VALUES
  ('自動検知', 'AUTO', '自動検知から起票'),
  ('手動登録', 'MANUAL', '手動で起票')
ON DUPLICATE KEY UPDATE name = VALUES(name), description = VALUES(description), updated_at = CURRENT_TIMESTAMP(6);

INSERT INTO case_statuses (name, value, description) VALUES
  ('起票', 'OPEN', '調査開始'),
  ('調査中', 'INVESTIGATING', '調査中'),
  ('要措置', 'ACTION_REQUIRED', '措置必要'),
  ('完了', 'CLOSED', '対応完了')
ON DUPLICATE KEY UPDATE name = VALUES(name), description = VALUES(description), updated_at = CURRENT_TIMESTAMP(6);

INSERT INTO risk_levels (name, value, description) VALUES
  ('低', 'LOW', '低リスク'),
  ('中', 'MEDIUM', '中リスク'),
  ('高', 'HIGH', '高リスク'),
  ('重大', 'CRITICAL', '重大リスク')
ON DUPLICATE KEY UPDATE name = VALUES(name), description = VALUES(description), updated_at = CURRENT_TIMESTAMP(6);

INSERT INTO account_action_types (name, value, description) VALUES
  ('凍結', 'FREEZE', '凍結措置'),
  ('凍結解除', 'UNFREEZE', '凍結解除'),
  ('退会処理', 'WITHDRAW', '退会処理')
ON DUPLICATE KEY UPDATE name = VALUES(name), description = VALUES(description), updated_at = CURRENT_TIMESTAMP(6);

INSERT INTO user_status_event_types (name, value, description) VALUES
  ('登録完了', 'REGISTERED', '本登録完了'),
  ('凍結', 'FROZEN', '凍結遷移'),
  ('凍結解除', 'UNFROZEN', '凍結解除遷移'),
  ('退会', 'WITHDRAWN', '退会遷移')
ON DUPLICATE KEY UPDATE name = VALUES(name), description = VALUES(description), updated_at = CURRENT_TIMESTAMP(6);
