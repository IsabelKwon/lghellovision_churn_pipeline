"""파이프라인 실행 결과 확인용 스크립트.

    python scripts/check_results.py

1. 정제 규칙이 적용됐는지 (행 수, 고객 수, NULL 변환, 캡핑)
2. 해지 관련 비율 (행 단위 비율, churn_base 기준 비율, 고객 단위 비율)
   행 단위 비율과 churn_base 기준 비율은 둘 다 월 해지율이 아니다.
   확실한 지표는 고객 단위 비율(customer_churn)이다.
   (scripts/investigate_churn_rows.py, README "해지 라벨 검증" 참고)
3. 세그먼트별 해지 고객 비율 (customer_churn 기준)
"""
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
CLEAN = f"read_parquet('{(ROOT / 'data/clean/churn_clean').as_posix()}/*/*.parquet', hive_partitioning = false)"
BASE = f"read_parquet('{(ROOT / 'data/mart/churn_base').as_posix()}/*/*.parquet', hive_partitioning = false)"
SEG = (ROOT / "data/mart/segment_churn.parquet").as_posix()
CUSTOMER = (ROOT / "data/mart/customer_churn.parquet").as_posix()

if not (ROOT / "data/mart/customer_churn.parquet").exists():
    sys.exit("결과 파일이 없습니다. 먼저 python run_pipeline.py 를 실행하세요.")

print("\n[1] 정제 결과 확인")
print(duckdb.sql(f"""
    SELECT
        count(*)                                          AS 행수,
        count(DISTINCT sha2_hash)                         AS 고객수,
        count(*) FILTER (WHERE INHOME_RATE IS NULL)       AS inhome_null,
        count(*) FILTER (WHERE SCRB_PATH_NM_GRP IS NULL)  AS scrb_path_null,
        max(CH_25_RATIO_MONTH1)                           AS ch25_max
    FROM {CLEAN}
"""))

print("\n[2] 해지 관련 비율 (행 단위, churn_base 기준 비율은 월 해지율이 아님)")
print(duckdb.sql(f"""
    SELECT
        (SELECT round(100 * avg((cancel_yn = '해지')::INT), 3) FROM {CLEAN}) AS 행단위_해지행_비율_pct,
        (SELECT round(100 * avg(is_churn), 3) FROM {BASE})                  AS churn_base_해지행_비율_pct,
        (SELECT round(100 * count(DISTINCT sha2_hash) FILTER (WHERE cancel_yn = '해지')
                / count(DISTINCT sha2_hash), 2) FROM {CLEAN})               AS 해지고객_비율_pct,
        (SELECT round(100 * avg(is_churner), 2) FROM '{CUSTOMER}')          AS customer_churn_해지고객_비율_pct,
        (SELECT sum(is_churner) FROM '{CUSTOMER}')                           AS 해지_고객수,
        (SELECT round(avg(n), 1) FROM (
            SELECT count(*) AS n FROM {CLEAN} WHERE cancel_yn = '해지' GROUP BY sha2_hash
        ))                                                                   AS 고객당_해지행_평균
"""))

print("\n[3] 세그먼트별 해지 고객 비율 (lift = 전체 해지 고객 비율 대비 배수)")
print(duckdb.sql(f"SELECT * FROM '{SEG}'").df().to_string())
