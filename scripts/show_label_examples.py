"""원본 CSV 에서 실제 고객 3명의 월별 기록을 그대로 보여주는 스크립트.

    python scripts/show_label_examples.py

investigate_churn_rows.py 는 전체 고객을 숫자로 세지만,
이 스크립트는 고객 한 명 한 명의 원본 줄을 눈으로 직접 확인하기 위한 것이다.
  - 유지 고객: 11개월 모두 '유지'
  - 해지 고객: 처음 나온 달부터 전부 '해지'. 중간에 '유지'가 없다
  - 충돌 고객: 매달 '유지'와 '해지' 두 줄이 같이 있다 (R3a 로 제외되는 고객)

원본을 읽기만 하고 아무것도 바꾸지 않는다. 고객 식별값은 앞 6자리만 보여준다.
"""
import argparse
import glob
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent

parser = argparse.ArgumentParser(description="원본 CSV 의 고객별 해지 라벨 확인")
parser.add_argument("--raw", default=str(ROOT / "data" / "raw" / "*.csv"), help="원본 CSV glob 패턴")
args = parser.parse_args()

RAW_FILES = sorted(glob.glob(args.raw))
if len(RAW_FILES) == 0:
    sys.exit(f"원본 CSV 가 없습니다: {args.raw}")

con = duckdb.connect(config={"memory_limit": "4GB", "threads": "2"})

# 원본에서 필요한 컬럼 3개만 읽는다 (고객, 달, 해지 여부)
file_list = "[" + ", ".join("'" + Path(f).as_posix().replace("'", "''") + "'" for f in RAW_FILES) + "]"
con.sql(f"""
    CREATE TABLE raw_labels AS
    SELECT sha2_hash, p_mt, cancel_yn
    FROM read_csv({file_list}, all_varchar = true, union_by_name = true)
""")

# 고객별 요약
#   keep_rows  : '유지' 줄 수
#   churn_rows : '해지' 줄 수
#   n_months   : 나온 달의 수
#   n_rows     : 전체 줄 수 (n_rows > n_months 이면 같은 달에 줄이 2개 이상 = 충돌)
con.sql("""
    CREATE TABLE per_customer AS
    SELECT
        sha2_hash,
        count(*) FILTER (WHERE cancel_yn = '유지') AS keep_rows,
        count(*) FILTER (WHERE cancel_yn = '해지') AS churn_rows,
        count(DISTINCT p_mt)                        AS n_months,
        count(*)                                    AS n_rows
    FROM raw_labels
    GROUP BY sha2_hash
""")

examples = [
    ("유지 고객", "churn_rows = 0 AND n_months = 11"),
    ("해지 고객 ('해지' 줄 6개 이상)", "keep_rows = 0 AND churn_rows >= 6 AND n_rows = n_months"),
    ("충돌 고객 (R3a 로 제외)", "keep_rows > 0 AND churn_rows > 0 AND n_rows > n_months"),
]

for title, condition in examples:
    found = con.sql(f"""
        SELECT sha2_hash FROM per_customer
        WHERE {condition}
        ORDER BY sha2_hash
        LIMIT 1
    """).fetchone()
    if found is None:
        print(f"\n== {title}: 조건에 맞는 고객이 없습니다")
        continue
    customer = found[0]

    rows = con.sql(f"""
        SELECT p_mt, cancel_yn
        FROM raw_labels
        WHERE sha2_hash = '{customer}'
        ORDER BY p_mt, cancel_yn
    """).fetchall()

    print(f"\n== {title}: 고객 {customer[:6]}... (원본 {len(rows)}줄)")
    for month, label in rows:
        print(f"     {month}  {label}")


print("\n== 전체 고객 중 '유지'와 '해지'가 둘 다 있는 고객 수 (같은 달이든 다른 달이든)")
both_labels, both_in_different_months_only = con.sql("""
    WITH month_status AS (
        SELECT sha2_hash, p_mt, count(DISTINCT cancel_yn) AS n_labels
        FROM raw_labels
        GROUP BY sha2_hash, p_mt
    )
    SELECT
        -- 유지와 해지를 둘 다 가진 고객
        (SELECT count(*) FROM per_customer WHERE keep_rows > 0 AND churn_rows > 0),
        -- 그중 "같은 달에 둘 다 있는 달"이 한 번도 없는 고객 = 달이 바뀌며 라벨이 바뀐 고객
        (SELECT count(*) FROM per_customer p
         WHERE p.keep_rows > 0 AND p.churn_rows > 0
           AND NOT EXISTS (SELECT 1 FROM month_status m
                           WHERE m.sha2_hash = p.sha2_hash AND m.n_labels > 1))
""").fetchone()
print(f"     유지와 해지를 둘 다 가진 고객          : {both_labels:,}명")
print(f"     그중 달이 바뀌며 라벨이 바뀐 고객      : {both_in_different_months_only:,}명")
if both_in_different_months_only == 0:
    print("     → 유지와 해지를 둘 다 가진 고객은 전부 '같은 달에 두 줄'인 충돌 고객이고,")
    print("       '유지였다가 다음 달에 해지'처럼 바뀐 고객은 없다. (cancel_yn 은 고객마다 고정된 값)")
else:
    print("     → 달이 바뀌며 라벨이 바뀐 고객이 있다. cancel_yn 이 월별로 바뀌는 데이터다.")
    print("       (scripts/make_sample_data.py 로 만든 샘플 데이터는 이렇게 만들어진다)")
