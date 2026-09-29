-- 01c_conflicted_customers.sql
-- [1단계-3] dup_rows 에 모은 중복 줄을 비교해서 3종류로 나누고, 통째로 뺄 고객을 정한다.
--
--   label_conflict  : cancel_yn(유지/해지)이 서로 다름        -> 고객 전체 제외 (R3a)
--   value_mismatch  : cancel_yn은 같은데 다른 컬럼 값이 다름   -> 고객 전체 제외 (R3c)
--   exact_duplicate : 모든 컬럼이 완전히 똑같음                -> 1줄만 남김     (R3b)
--
-- EDA(01_eda.ipynb 3-3절)에서 확인한 것
--   - 실제 데이터의 중복은 전부 label_conflict (cancel_yn 하나만 다름)
--   - 한 번 충돌한 고객은 그 뒤 모든 달에서도 계속 충돌함
--   - 어느 쪽 라벨이 맞는지 알 수 없어서 그 고객을 통째로 뺌 (40,063명, 1.84%)


-- (고객, 달)마다 어떤 종류의 중복인지 판단
--   해시 종류가 1개면 "완전히 같은 줄", 2개 이상이면 "어딘가 다른 줄"이다.
CREATE OR REPLACE TABLE dup_groups AS
SELECT
    sha2_hash,
    p_mt,
    count(*) AS n_rows,
    CASE
        WHEN count(DISTINCT cancel_yn) > 1 THEN 'label_conflict'
        WHEN count(DISTINCT row_hash) > 1 THEN 'value_mismatch'
        ELSE 'exact_duplicate'
    END AS dup_type
FROM dup_rows
GROUP BY sha2_hash, p_mt;


-- 통째로 뺄 고객 목록 (label_conflict 또는 value_mismatch 가 한 번이라도 있는 고객)
--   둘 다 있는 고객은 label_conflict 로 기록한다.
--   exact_duplicate 만 있는 고객은 여기 넣지 않는다. (02_drop_rules.sql 에서 1줄만 남김)
CREATE OR REPLACE TABLE conflicted_customers AS
SELECT
    sha2_hash,
    CASE
        WHEN sum(CASE WHEN dup_type = 'label_conflict' THEN 1 ELSE 0 END) > 0 THEN 'label_conflict'
        ELSE 'value_mismatch'
    END AS dup_type
FROM dup_groups
WHERE dup_type = 'label_conflict' OR dup_type = 'value_mismatch'
GROUP BY sha2_hash;
