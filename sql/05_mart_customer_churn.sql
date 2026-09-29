-- 05_mart_customer_churn.sql
-- 고객 1명 = 1행으로 요약한 테이블. 해지 고객 비율과 세그먼트별 해지율의 기준이 된다.
--
-- 왜 만들었나 (scripts/investigate_churn_rows.py, README "해지 라벨 검증" 참고)
--   cancel_yn 은 달마다 바뀌는 값이 아니라 고객마다 고정된 라벨이다. ('유지' <-> '해지'로 바뀐 고객 0명)
--   그래서 "몇 행이 해지냐"가 아니라 "몇 명이 해지 고객이냐"로 세야 한다.
--   행으로 세면 유지 고객이 오래 관측될수록 해지 비율이 낮아 보인다.
--     예: 유지 고객 A 11개월 + 해지 고객 B 1행 -> 행 기준 1/12 = 8.3%, 고객 기준 1/2 = 50%
--
-- 컬럼
--   is_churner        : 관측 기간 중 '해지' 라벨이 붙은 고객이면 1, 아니면 0
--   first_observed_mt : 데이터에 처음 나온 달
--   last_observed_mt  : 데이터에 마지막으로 나온 달
--                       (사실만 기록한다. "해지한 달"이라는 뜻이 아니다. 해지 시점은 추정이라 넣지 않음)
--   n_months          : 데이터에 나온 달의 수
--   세그먼트 컬럼     : 처음 나온 달의 값 (config.SEGMENT_COLS)
--                       마지막 달 값을 쓰면 해지 직전의 해지 문의 급증 같은 "해지의 결과"가 섞인다.
--
-- 아래 segment_cols 자리(중괄호로 감싼 부분)는 run.py 의 make_customer_segment_cols() 가 채운다.

WITH summary AS (
    SELECT
        sha2_hash,
        max(CASE WHEN cancel_yn = '해지' THEN 1 ELSE 0 END) AS is_churner,
        min(p_mt)                                           AS first_observed_mt,
        max(p_mt)                                           AS last_observed_mt,
        count(*)                                            AS n_months
    FROM clean
    GROUP BY sha2_hash
)

-- (sha2_hash, p_mt)는 정제 데이터에서 유일하다(primary_key_unique 검사).
-- 그래서 처음 나온 달의 행과 JOIN 하면 고객마다 정확히 1행이 나온다.
SELECT
    s.sha2_hash,
    s.is_churner,
    s.first_observed_mt,
    s.last_observed_mt,
    s.n_months,
{segment_cols}
FROM summary s
JOIN clean c
  ON c.sha2_hash = s.sha2_hash
 AND c.p_mt = s.first_observed_mt
