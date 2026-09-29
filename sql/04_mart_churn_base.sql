-- 04_mart_churn_base.sql
-- 정제 데이터(clean)로 해지 분석/모델링에 쓸 기본 테이블을 만든다.
--
-- 규칙: 고객마다 첫 '해지' 행까지만 남기고, 그 이후 달은 버린다. (해지 고객 1명당 해지 행 1개)
--
-- 이유
--   - 해지 고객 1명은 여러 달에 걸쳐 '해지' 행으로 나온다. (1명당 평균 5.9개, 첫 행 이후 739,092개)
--     그대로 두면 같은 고객을 여러 번 센다.
--
-- 주의 (scripts/investigate_churn_rows.py 로 확인)
--   처음에는 "첫 '해지' 행 = 해지가 일어난 달"로 보고 설계했지만, 이 해석은 틀렸다.
--   - cancel_yn 은 고객마다 고정된 값이다. '유지' -> '해지'로 바뀐 고객이 원본 전체에 0명.
--   - 해지 고객의 96.1% 는 데이터 첫 달(202302)부터 이미 '해지'다.
--   그래서 first_churn_mt 는 대부분 "해지한 달"이 아니라 "데이터에 처음 나온 달"이다.
--   해지 시점을 어떻게 정할지는 cancel_yn 정의를 확인한 뒤 정할 과제로 남겼다. (README 한계 참고)
--
-- 이 규칙을 정제 단계가 아니라 마트 단계에 둔 이유
--   - 정제 데이터는 "원본을 깨끗하게 만든 것"이라 모든 줄을 남겨둔다. (다른 분석에 쓸 수 있음)
--   - 행을 자르는 규칙은 해지 분석이라는 특정 목적에만 필요하다.
--
-- 새로 추가하는 컬럼
--   is_churn       : 이 행이 그 고객의 첫 '해지' 행이면 1, 아니면 0
--   first_churn_mt : 이 고객의 첫 '해지' 행의 달 (해지 고객이 아니면 NULL)

-- 고객별 첫 '해지' 행의 달
WITH first_churn AS (
    SELECT sha2_hash, min(p_mt) AS first_churn_mt
    FROM clean
    WHERE cancel_yn = '해지'
    GROUP BY sha2_hash
)

SELECT
    c.*,
    CAST(c.cancel_yn = '해지' AS INTEGER) AS is_churn,   -- 참이면 1, 거짓이면 0
    f.first_churn_mt
FROM clean c
LEFT JOIN first_churn f ON c.sha2_hash = f.sha2_hash
WHERE f.first_churn_mt IS NULL        -- 해지한 적 없는 고객은 전부 남김
   OR c.p_mt <= f.first_churn_mt      -- 해지한 고객은 첫 해지 달까지만 남김
