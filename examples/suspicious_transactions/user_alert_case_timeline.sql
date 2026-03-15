USE exchange_domain;

/*
  目的:
  - ユーザー単位で、検知、ケース起票、口座措置、状態変更を時系列で1本に並べる。
  - AMLレビュー時に「何が、どの順で起きたか」を追いやすくする。

  見方:
  - timeline_events で異なる業務イベントを
    (user_id, event_at, event_type, event_id, primary_label, secondary_label)
    の共通フォーマットへ正規化している。
  - たとえば同一ユーザーで
    ALERT_DETECTED -> CASE_OPENED -> ACCOUNT_ACTION -> USER_STATUS_CHANGED
    の順に並べば、検知から措置までの流れがそのまま追える。

  実装上の注意:
  - タイムライン表示用の一覧化は SQL が得意だが、
    「同種イベントのグルーピング」「画面表示向けの折りたたみ」「差分ハイライト」まで
    1本のSQLでやり切ると可読性が落ちやすい。
  - UI向けの整形やイベント統合ルールは、
    手続き型コード側で組み立てる前提にした方が保守しやすい。
*/
WITH timeline_events AS (
  -- 自動検知イベント。
  -- primary_label にルール名、secondary_label にスコアと検知ステータスを入れる。
  SELECT
    ael.user_id,
    ael.detected_at AS event_at,
    'ALERT_DETECTED' AS event_type,
    ael.id AS event_id,
    ar.rule_name AS primary_label,
    CONCAT('score=', ael.score, ', status=', aes.value) AS secondary_label
  FROM alert_event_logs ael
  INNER JOIN alert_rules ar ON ar.id = ael.rule_id
  INNER JOIN alert_event_statuses aes ON aes.id = ael.alert_event_status_id

  UNION ALL

  -- ケース起票イベント。
  -- primary_label にケースタイトル、secondary_label に現在ステータスとリスクを入れる。
  SELECT
    sc.user_id,
    sc.opened_at AS event_at,
    'CASE_OPENED' AS event_type,
    sc.id AS event_id,
    sc.title AS primary_label,
    CONCAT('status=', cs.value, ', risk=', rl.value) AS secondary_label
  FROM suspicious_cases sc
  INNER JOIN case_statuses cs ON cs.id = sc.current_status_id
  INNER JOIN risk_levels rl ON rl.id = sc.risk_level_id

  UNION ALL

  -- 口座措置イベント。
  -- primary_label に措置種別、secondary_label に措置理由を入れる。
  SELECT
    aa.user_id,
    aa.requested_at AS event_at,
    'ACCOUNT_ACTION' AS event_type,
    aa.id AS event_id,
    aat.value AS primary_label,
    aa.action_reason AS secondary_label
  FROM account_actions aa
  INNER JOIN account_action_types aat ON aat.id = aa.action_type_id

  UNION ALL

  -- ユーザーステータス変更イベント。
  -- primary_label にイベント種別(FROZEN など)、secondary_label に変更理由を入れる。
  SELECT
    usce.user_id,
    usce.occurred_at AS event_at,
    'USER_STATUS_CHANGED' AS event_type,
    usce.id AS event_id,
    uset.value AS primary_label,
    usce.reason AS secondary_label
  FROM user_status_change_events usce
  INNER JOIN user_status_event_types uset ON uset.id = usce.event_type_id
)
SELECT
  -- どのユーザーのタイムラインか。
  u.id AS user_id,
  u.member_code,
  -- いつ起きたイベントか。
  te.event_at,
  -- ALERT_DETECTED / CASE_OPENED / ACCOUNT_ACTION / USER_STATUS_CHANGED の種別。
  te.event_type,
  -- 元テーブル上のID。詳細確認時の手掛かりになる。
  te.event_id,
  -- 一次表示用ラベル。
  te.primary_label,
  -- 補足情報。
  te.secondary_label
FROM timeline_events te
INNER JOIN users u ON u.id = te.user_id
-- 新しいイベントを先頭にし、同時刻なら user_id -> event_type -> event_id で並びを安定化する。
ORDER BY te.event_at DESC, u.id, te.event_type, te.event_id;
