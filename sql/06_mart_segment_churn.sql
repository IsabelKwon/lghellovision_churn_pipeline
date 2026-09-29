-- 06_mart_segment_churn.sql
-- 세그먼트(그룹)별 해지 고객 비율을 계산한 요약 테이블
--
-- churn_rate_pct = 그룹의 해지 고객 수 / 그룹의 고객 수 * 100  (customer_churn 기준, 고객 1명 = 1번)
-- lift = 그룹의 해지 고객 비율 / 전체 해지 고객 비율
--   1보다 크면 평균보다 해지 고객 비율이 높은 그룹이다.
-- 세그먼트 값은 고객이 처음 나온 달의 값이다. (05_mart_customer_churn.sql 참고)
--
-- 바꾼 이유
--   처음에는 churn_base 의 행으로 셌다. (해지 행 수 / 전체 행 수)
--   그런데 cancel_yn 은 고객마다 고정된 라벨이라, 행으로 세면 유지 고객이 오래 관측된 그룹일수록
--   비율이 낮아 보인다. 실제 데이터에서 고객 기준으로 다시 계산해 보니
--   약정 구간 세그먼트의 순위가 바뀌었다. (예: 약정만료전 6~9개월 행 기준 10위 -> 고객 기준 5위)
--
-- 아래 seg 안의 segment_unions 자리(중괄호로 감싼 부분)는 run.py 의 make_segment_unions() 가 채운다.
--   (주의: 주석 안에 중괄호를 쓰면 파이썬 format() 이 그것까지 바꿔버려서 여기엔 안 썼다)
--   config.py 의 SEGMENT_COLS 에 있는 컬럼마다 아래 같은 SELECT 를 하나씩 만들어서 UNION ALL 로 이어 붙인다.
--     SELECT 'BUNDLE_YN' AS segment_col, BUNDLE_YN AS segment_value, count(*), sum(is_churner)
--     FROM customer_churn GROUP BY ...
--   그래서 SEGMENT_COLS 에 컬럼을 추가하기만 하면 이 테이블에도 자동으로 들어간다.
--
-- 결과 모양 (세로로 긴 형태)
--   segment_col | segment_value | customers | churners | churn_rate_pct | lift
--   BUNDLE_YN   | Y             | ...

WITH overall AS (
    -- 전체 해지 고객 비율
    SELECT avg(is_churner) AS overall_rate
    FROM customer_churn
),
seg AS (
{segment_unions}
)
SELECT
    seg.segment_col,
    seg.segment_value,
    seg.customers,
    seg.churners,
    round(100.0 * seg.churners / seg.customers, 3) AS churn_rate_pct,
    round(seg.churners / seg.customers / overall.overall_rate, 3) AS lift
FROM seg, overall
ORDER BY seg.segment_col, lift DESC
