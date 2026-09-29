"""'해지' 라벨(cancel_yn)이 실제로 무엇을 뜻하는지 확인하는 스크립트.

    python scripts/investigate_churn_rows.py

처음 가정
    고객이 '유지'로 지내다가 어느 달에 '해지'로 바뀌고, 그 뒤에도 '해지' 행이 남는다.
    → 그래서 마트(churn_base)는 "첫 해지 달 = 해지한 달"로 보고, 그 달까지만 남겼다.

이 스크립트로 확인한 것
    1. 행 단위로 세면 해지 고객 1명이 여러 번 세진다
    2. 그런데 '유지' → '해지'로 바뀐 고객은 원본 전체에 한 명도 없다     <- 가정이 틀렸다
    3. 해지 고객 대부분은 데이터 첫 달부터 이미 '해지'다
    4. 해지 고객은 달이 지나며 데이터에서 하나씩 사라진다
    5. 해지 고객도 그 기간에 서비스를 쓰고 있었고, 사라지기 전에 이용이 줄어든다
    → cancel_yn 은 "그 달에 해지했다"가 아니라 고객마다 고정된 값처럼 동작한다.
      "첫 해지 달"은 대부분 해지한 달이 아니라 데이터가 시작된 달이다.

정제 데이터(data/clean/churn_clean)와 원본 CSV(data/raw)를 읽기만 하고, 아무것도 바꾸지 않는다.
고객 식별값(sha2_hash)은 화면에 찍지 않는다.
"""
import glob
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
CLEAN_DIR = ROOT / "data" / "clean" / "churn_clean"
RAW_FILES = sorted(glob.glob(str(ROOT / "data" / "raw" / "*.csv")))

if not CLEAN_DIR.exists():
    sys.exit("정제 데이터가 없습니다. 먼저 python run_pipeline.py 를 실행하세요.")

con = duckdb.connect(config={"memory_limit": "4GB", "threads": "2"})
con.sql(f"""
    CREATE VIEW clean AS
    SELECT * FROM read_parquet('{CLEAN_DIR.as_posix()}/*/*.parquet', hive_partitioning = false)
""")

# 고객별 요약: '유지' 행 수, '해지' 행 수, 처음/마지막으로 나온 달
con.sql("""
    CREATE TEMP TABLE per_customer AS
    SELECT
        sha2_hash,
        count(*) FILTER (WHERE cancel_yn = '유지') AS keep_rows,
        count(*) FILTER (WHERE cancel_yn = '해지') AS churn_rows,
        min(p_mt) AS first_mt,
        max(p_mt) AS last_mt
    FROM clean
    GROUP BY sha2_hash
""")
first_month, last_month = con.sql("SELECT min(p_mt), max(p_mt) FROM clean").fetchone()


print("\n[1] 행 단위로 세면 해지 고객 1명이 여러 번 세진다")
total_rows, churn_rows = con.sql("""
    SELECT count(*), count(*) FILTER (WHERE cancel_yn = '해지') FROM clean
""").fetchone()
total_customers, churn_customers, avg_churn_rows = con.sql("""
    SELECT count(*), count(*) FILTER (WHERE churn_rows > 0), avg(churn_rows) FILTER (WHERE churn_rows > 0)
    FROM per_customer
""").fetchone()
print(f"    전체 {total_rows:,}행 중 '해지' {churn_rows:,}행 = {100 * churn_rows / total_rows:.3f}%")
print(f"    해지 고객 {churn_customers:,}명, 1명당 '해지' 행 평균 {avg_churn_rows:.1f}개")
print("    → 행 비율(4%대)은 해지율이 아니다. 같은 고객을 여러 번 센다.")


print("\n[2] 정제 데이터: 한 고객 안에 '유지'와 '해지'가 둘 다 있는 경우 (상태가 바뀐 고객)")
changed_clean = con.sql("""
    SELECT count(*) FROM per_customer WHERE keep_rows > 0 AND churn_rows > 0
""").fetchone()[0]
print(f"    {changed_clean:,}명")

print("\n    원본 CSV 전체에서도 확인 (충돌 고객 포함, 컬럼 3개만 읽음)")
raw_list = "[" + ", ".join("'" + Path(f).as_posix().replace("'", "''") + "'" for f in RAW_FILES) + "]"
con.sql(f"""
    CREATE TEMP TABLE raw_month_status AS
    SELECT
        sha2_hash,
        p_mt,
        CASE
            WHEN count(DISTINCT cancel_yn) > 1 THEN 'B'   -- 같은 달에 유지+해지 둘 다 (충돌)
            WHEN max(cancel_yn) = '해지' THEN 'C'         -- 해지만
            ELSE 'K'                                      -- 유지만
        END AS status
    FROM read_csv({raw_list}, all_varchar = true, union_by_name = true)
    GROUP BY sha2_hash, p_mt
""")
# 고객마다 달 순서대로 상태를 이어 붙인다. 예: 'KKKCC' = 유지 3달 뒤 해지 2달
raw_patterns = con.sql("""
    WITH seq AS (
        SELECT sha2_hash, string_agg(status, '' ORDER BY p_mt) AS pattern
        FROM raw_month_status
        GROUP BY sha2_hash
    )
    SELECT
        count(*)                                                    AS customers,
        count(*) FILTER (WHERE pattern NOT LIKE '%C%' AND pattern NOT LIKE '%B%') AS always_keep,
        count(*) FILTER (WHERE pattern NOT LIKE '%K%' AND pattern NOT LIKE '%B%') AS always_churn,
        count(*) FILTER (WHERE pattern NOT LIKE '%K%' AND pattern NOT LIKE '%C%') AS always_both,
        count(*) FILTER (WHERE regexp_matches(pattern, 'K.*[CB]|[CB].*K|C.*B|B.*C')) AS status_changed
    FROM seq
""").fetchone()
customers, always_keep, always_churn, always_both, status_changed = raw_patterns
print(f"    원본 고객 {customers:,}명")
print(f"      처음부터 끝까지 '유지'만          : {always_keep:>9,}명")
print(f"      처음부터 끝까지 '해지'만          : {always_churn:>9,}명")
print(f"      처음부터 끝까지 '유지+해지' 동시  : {always_both:>9,}명  (충돌 고객, R3a 로 제외)")
print(f"      중간에 상태가 바뀐 고객           : {status_changed:>9,}명")
if status_changed == 0:
    print("    → '유지'였다가 '해지'로 바뀌는 순간이 데이터에 없다.")
    print("      cancel_yn 은 달마다 바뀌는 상태가 아니라, 고객마다 고정된 값처럼 동작한다.")
else:
    print("    → 상태가 바뀐 고객이 있다. 이 데이터에서는 cancel_yn 이 월별로 바뀐다.")
    print("      (scripts/make_sample_data.py 로 만든 샘플 데이터는 이렇게 만들어진다)")


print(f"\n[3] 해지 고객이 처음 나온 달 (데이터는 {first_month} ~ {last_month})")
from_first_month = con.sql(f"""
    SELECT count(*) FROM per_customer WHERE churn_rows > 0 AND first_mt = '{first_month}'
""").fetchone()[0]
print(f"    해지 고객 {churn_customers:,}명 중 {from_first_month:,}명({100 * from_first_month / churn_customers:.1f}%)이 "
      f"첫 달({first_month})부터 이미 '해지'")
print("    → 마트의 '첫 해지 달'은 대부분 해지한 달이 아니라 데이터가 시작된 달이다.")


print("\n[4] 달별로 남아 있는 고객 수: 해지 고객은 하나씩 사라진다")
monthly = con.sql("""
    SELECT
        p_mt,
        count(*) FILTER (WHERE cancel_yn = '유지') AS keep_customers,
        count(*) FILTER (WHERE cancel_yn = '해지') AS churn_customers
    FROM clean
    GROUP BY p_mt
    ORDER BY p_mt
""").fetchall()
print("      달       '유지' 고객   '해지' 고객")
for month, keep_n, churn_n in monthly:
    print(f"      {month}  {keep_n:>11,}  {churn_n:>11,}")
print("    → '유지' 고객 수는 거의 그대로인데, '해지' 고객은 달마다 줄어든다.")
print("      실제 해지는 데이터에서 사라지는 무렵에 일어났을 가능성이 높다. (추정)")


print("\n[5] 해지 고객도 그 기간에 서비스를 쓰고 있었나 (1개월 평균 시청시간 기준)")
usage = con.sql("""
    SELECT
        CASE
            WHEN c.cancel_yn = '유지' THEN '유지 고객'
            WHEN c.p_mt = p.last_mt THEN '해지 고객: 마지막으로 나온 달'
            ELSE '해지 고객: 그 전 달들'
        END AS grp,
        count(*)                                           AS rows,
        avg(CH_HH_AVG_MONTH1)                              AS avg_view,
        100.0 * avg(CASE WHEN CH_HH_AVG_MONTH1 = 0 THEN 1 ELSE 0 END) AS pct_zero_view
    FROM clean c
    JOIN per_customer p ON c.sha2_hash = p.sha2_hash
    GROUP BY grp
    ORDER BY grp DESC
""").fetchall()
for grp, rows, avg_view, pct_zero in usage:
    print(f"      {grp:<22s} {rows:>11,}행  평균 시청 {avg_view:4.2f}  시청 0인 비율 {pct_zero:4.1f}%")
print("    → '해지' 행인 달에도 대부분 시청하고 있었다. 그 달에 이미 해지한 상태가 아니다.")
print("      사라지기 직전으로 갈수록 이용이 줄어든다.")


print("\n[6] 해지 고객은 언제 데이터에서 사라지나 (해지 후 N개월 뒤 삭제하는 규칙이 있는지 확인)")
# 12월(마지막 달)까지 남은 고객은 사라진 게 아니라 데이터가 끝난 것이므로 제외한다
con.sql(f"""
    CREATE TEMP TABLE disappeared AS
    SELECT sha2_hash, last_mt
    FROM per_customer
    WHERE churn_rows > 0 AND last_mt < '{last_month}'
""")
n_disappeared = con.sql("SELECT count(*) FROM disappeared").fetchone()[0]
n_keep = total_customers - churn_customers
n_keep_disappeared = con.sql(f"""
    SELECT count(*) FROM per_customer WHERE churn_rows = 0 AND last_mt < '{last_month}'
""").fetchone()[0]
print(f"    {last_month} 전에 사라진 고객: 해지 고객 {n_disappeared:,} / {churn_customers:,}명 "
      f"({100 * n_disappeared / churn_customers:.1f}%), "
      f"유지 고객 {n_keep_disappeared:,} / {n_keep:,}명 ({100 * n_keep_disappeared / n_keep:.2f}%)")

# 사라지기 몇 달 전인지(0 = 마지막 달)별로 해지·정지 문의 비율과 시청을 본다
# "해지 후 3개월 뒤 삭제"라면 해지 문의가 사라지기 3개월 전에 몰려 있어야 한다
by_offset = con.sql("""
    SELECT
        (CAST(left(d.last_mt, 4) AS INT) * 12 + CAST(right(d.last_mt, 2) AS INT))
        - (CAST(left(c.p_mt, 4) AS INT) * 12 + CAST(right(c.p_mt, 2) AS INT)) AS months_before_last,
        count(*) AS rows,
        100.0 * avg(CASE WHEN VOC_STOP_CANCEL_MONTH1_YN = 'Y' THEN 1 ELSE 0 END) AS pct_cancel_voc,
        avg(CH_HH_AVG_MONTH1) AS avg_view
    FROM clean c
    JOIN disappeared d ON c.sha2_hash = d.sha2_hash
    GROUP BY months_before_last
    HAVING months_before_last <= 6
    ORDER BY months_before_last
""").fetchall()
print("      사라지기 몇 달 전   행 수      해지·정지 문의   평균 시청")
for offset, rows, pct_voc, avg_view in by_offset:
    label = "0 (마지막 달)" if offset == 0 else str(offset)
    print(f"      {label:<14s} {rows:>9,}      {pct_voc:5.1f}%          {avg_view:.2f}")

last_voc = by_offset[0][2]
earlier_voc = con.sql("""
    SELECT 100.0 * avg(CASE WHEN VOC_STOP_CANCEL_MONTH1_YN = 'Y' THEN 1 ELSE 0 END)
    FROM clean c JOIN per_customer p ON c.sha2_hash = p.sha2_hash
    WHERE p.churn_rows > 0 AND c.p_mt < p.last_mt
""").fetchone()[0]
print(f"    사라지기 직전 달의 해지·정지 문의 {last_voc:.1f}% = 같은 해지 고객의 이전 달들({earlier_voc:.1f}%)의 "
      f"약 {last_voc / earlier_voc:.1f}배")
print("    → 해지 문의가 마지막 달에만 몰려 있다. 'N개월 뒤 삭제'보다 '해지한 달을 끝으로 빠진다'에 가깝다. (추정)")


print("\n[정리]")
print(f"    확실하게 말할 수 있는 숫자: 관측 기간 중 해지 라벨이 붙은 고객 비율 "
      f"{churn_customers:,} / {total_customers:,} = {100 * churn_customers / total_customers:.2f}%")
print("    마트의 '첫 해지 달까지만' 기준으로 계산한 비율(약 0.7%)은 월 해지율이 아니다.")
print("    cancel_yn 의 정확한 정의(예: '관측 기간 중 해지 여부')는 데이터 제공처 확인이 필요하다.")
