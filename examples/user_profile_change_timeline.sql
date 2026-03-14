USE exchange_domain;

/*
  目的:
  - ユーザープロフィール履歴を版順に確認し、属性変更の監査に使う。
  - 変更理由と declared_at をあわせて、KYC 再申告の流れを追いやすくする。

  見方:
  - user_profile_versions の1行を「その時点で申告されたプロフィールの版」として扱う。
  - version_no が 1 -> 2 と増えていれば、同一ユーザーの再申告・更新履歴を追える。
*/
SELECT
  -- どのユーザーの履歴か。
  u.id AS user_id,
  -- 業務で参照しやすい会員コード。
  u.member_code,
  -- プロフィールの版番号。大きいほど新しい申告。
  upv.version_no,
  -- その版で申告された氏名。
  upv.last_name,
  upv.first_name,
  -- その版で申告された居住国コード。
  upv.country_code,
  -- occupation_id をマスタ値に変換した職業。
  o.value AS occupation,
  -- 年収帯マスタの表示値。
  aib.value AS annual_income_bracket,
  -- 金融資産帯マスタの表示値。
  fab.value AS financial_asset_bracket,
  -- 変更理由。例: 引越し、転職、資産状況更新など。
  upv.change_reason,
  -- いつこの版を申告したか。
  upv.declared_at
FROM user_profile_versions upv
INNER JOIN users u ON u.id = upv.user_id
INNER JOIN occupations o ON o.id = upv.occupation_id
INNER JOIN annual_income_brackets aib ON aib.id = upv.annual_income_bracket_id
INNER JOIN financial_asset_brackets fab ON fab.id = upv.financial_asset_bracket_id
ORDER BY
  -- 直近の申告から見たいので declared_at 降順。
  upv.declared_at DESC,
  -- 同一日時ならユーザー単位でまとまるようにする。
  u.id,
  -- 同一ユーザー内では新しい版を先に出す。
  upv.version_no DESC;
