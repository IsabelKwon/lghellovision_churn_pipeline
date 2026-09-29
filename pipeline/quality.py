"""정제 결과와 마트를 자동으로 검사하는 함수들.

노트북에서 눈으로 확인하던 것들을 "통과 / 실패"가 나오는 함수로 바꿨다.

검사 함수는 모두 아래 모양의 딕셔너리를 돌려준다.
    {
        "name": 검사 이름,
        "passed": True 또는 False,
        "detail": 로그에 찍을 설명 문장,
        "metrics": 리포트(JSON)에 저장할 숫자들,
        "blocking": True 이면 실패 시 배포를 멈춘다. False 이면 경고만 한다.
    }

blocking 검사가 하나라도 실패하면 run.py 는 결과를 최종 폴더로 옮기지 않는다.
(그래서 기존에 있던 정상 데이터가 그대로 유지된다)
"""
from pipeline import config


def make_result(name, passed, detail, metrics, blocking=True):
    return {"name": name, "passed": passed, "detail": detail, "metrics": metrics, "blocking": blocking}


def query_one_row(con, sql):
    """SQL 을 실행하고 첫 번째 줄을 튜플로 돌려준다."""
    return con.sql(sql).fetchone()


# =====================================================================
# 정제 데이터(clean) 검사
# =====================================================================

def check_drop_reconciliation(con, raw_rows, drop_counts):
    """원본 줄 수 = 정제 줄 수 + 제외된 줄 수 인지 확인한다. (행 대사)

    - 합이 안 맞으면: 규칙에 없는 이유로 줄이 사라지거나 생긴 것 (코드 버그)
    - 한 규칙의 제외 비율이 상한을 넘으면: 그 규칙이 평소와 다르게 동작한 것 (데이터가 바뀜)
    - 전체 제외 비율도 상한을 넘으면 실패
    """
    clean_rows = query_one_row(con, "SELECT count(*) FROM clean")[0]
    dropped = sum(drop_counts.values())
    balanced = (raw_rows == clean_rows + dropped)

    if raw_rows > 0:
        drop_rate = dropped / raw_rows
    else:
        drop_rate = 1.0

    # 규칙마다 상한을 넘었는지 확인
    over_limit = {}
    for rule, limit in config.RULE_MAX_DROP_RATE.items():
        if raw_rows > 0:
            rule_rate = drop_counts.get(rule, 0) / raw_rows
        else:
            rule_rate = 0
        if rule_rate > limit:
            over_limit[rule] = f"{rule_rate:.3%} > 상한 {limit:.1%}"

    # config 에 상한이 없는 사유가 나오면 실패 (SQL 에만 규칙을 추가하고 config 를 깜빡한 경우)
    unknown_rules = []
    for rule in drop_counts:
        if rule not in config.RULE_MAX_DROP_RATE:
            unknown_rules.append(rule)
    unknown_rules.sort()

    passed = balanced and len(over_limit) == 0 and len(unknown_rules) == 0 and drop_rate <= config.MAX_DROP_RATE

    # 로그 문장 만들기
    rule_texts = []
    for rule in sorted(drop_counts):
        rule_texts.append(f"{rule} {drop_counts[rule]:,}")
    if rule_texts:
        by_rule = ", ".join(rule_texts)
    else:
        by_rule = "없음"

    if balanced:
        balance_text = "일치"
    else:
        balance_text = "불일치!"

    detail = (f"원본 {raw_rows:,} = 정제 {clean_rows:,} + 제외 {dropped:,} "
              f"({balance_text}, 제외율 {drop_rate:.2%}) | {by_rule}")
    if over_limit:
        detail += f" | 상한 초과: {over_limit}"
    if unknown_rules:
        detail += f" | 상한이 정의되지 않은 사유: {unknown_rules}"

    metrics = {
        "raw_rows": raw_rows,
        "clean_rows": clean_rows,
        "dropped_by_rule": drop_counts,
        "drop_rate": round(drop_rate, 6),
        "balanced": balanced,
        "over_limit": over_limit,
    }
    return make_result("drop_reconciliation", passed, detail, metrics)


def check_primary_key(con):
    """sha2_hash + p_mt 가 비어 있지 않고, 겹치는 게 없는지 (고객 1명당 한 달에 1줄)."""
    null_keys = query_one_row(con, """
        SELECT count(*) FROM clean
        WHERE sha2_hash IS NULL OR p_mt IS NULL
    """)[0]

    dup_groups = query_one_row(con, """
        SELECT count(*) FROM (
            SELECT sha2_hash, p_mt
            FROM clean
            GROUP BY sha2_hash, p_mt
            HAVING count(*) > 1
        )
    """)[0]

    passed = (dup_groups == 0 and null_keys == 0)
    detail = f"키 NULL {null_keys:,}행, 중복 (sha2_hash, p_mt) 그룹 {dup_groups:,}개"
    metrics = {"null_keys": null_keys, "dup_groups": dup_groups}
    return make_result("primary_key_unique", passed, detail, metrics)


def check_agmt_date_split(con):
    """AGMT_END_YMD 를 날짜로 바꾸는 규칙(R9)이 빠짐없이 적용됐는지.

    '무약정'도 NULL 도 아닌 값은 전부 날짜여야 한다.
    그런데 AGMT_END_DATE 가 NULL 이면 날짜 변환에 실패한 것이다.
    (TRY_STRPTIME 은 실패해도 에러 없이 NULL 을 돌려줘서 따로 확인해야 함)
    """
    row = query_one_row(con, """
        SELECT
            count(*) FILTER (WHERE AGMT_END_YMD IS NOT NULL
                               AND AGMT_END_YMD <> '무약정'
                               AND AGMT_END_DATE IS NULL) AS parse_failures,
            count(AGMT_END_DATE)                          AS dates,
            count(*) FILTER (WHERE IS_NO_AGMT)            AS no_agmt,
            min(AGMT_END_DATE)                            AS min_date,
            max(AGMT_END_DATE)                            AS max_date
        FROM clean
    """)
    parse_failures, dates, no_agmt, min_date, max_date = row

    passed = (parse_failures == 0)
    detail = (f"날짜 변환 실패 {parse_failures:,}건 | 날짜 {dates:,}건 ({min_date} ~ {max_date}), "
              f"무약정 {no_agmt:,}건")
    metrics = {
        "parse_failures": parse_failures,
        "dates": dates,
        "no_agmt": no_agmt,
        "min_date": str(min_date),
        "max_date": str(max_date),
    }
    return make_result("agmt_date_split", passed, detail, metrics)


def check_no_conflicted_customers(con):
    """통째로 빼야 하는 고객(R3a, R3c)이 정제 결과에 한 명도 없는지.

    완전 중복(R3b)만 있는 고객은 1줄씩 남기기 때문에 여기서 보지 않는다.
    그 고객의 중복이 잘 빠졌는지는 check_primary_key 가 확인한다.
    """
    remaining = query_one_row(con, """
        SELECT count(DISTINCT c.sha2_hash)
        FROM clean c
        JOIN conflicted_customers x ON c.sha2_hash = x.sha2_hash
    """)[0]
    total_conflicted = query_one_row(con, "SELECT count(*) FROM conflicted_customers")[0]

    passed = (remaining == 0)
    detail = f"충돌 고객 {total_conflicted:,}명 중 정제 결과에 남은 고객 {remaining:,}명"
    metrics = {"conflicted_customers": total_conflicted, "remaining": remaining}
    return make_result("no_conflicted_customers", passed, detail, metrics)


def get_varchar_columns(con):
    """clean 테이블에서 문자열(VARCHAR) 타입인 컬럼 이름 목록."""
    columns = []
    for row in con.sql("DESCRIBE clean").fetchall():
        column_name = row[0]
        column_type = row[1]
        if column_type == "VARCHAR":
            columns.append(column_name)
    return columns


def check_forbidden_sentinels(con):
    """'정보없음', '알수없음', '""', '10대미만' 이 어떤 문자열 컬럼에도 남아 있지 않은지.

    컬럼마다 쿼리를 따로 돌리면 2천만 줄을 여러 번 읽어야 해서,
    컬럼별 count 를 한 SELECT 에 전부 넣어 테이블을 한 번만 읽는다.
      SELECT count(*) FILTER (WHERE "컬럼1" IN (...)),
             count(*) FILTER (WHERE "컬럼2" IN (...)), ...
      FROM clean
    """
    varchar_cols = get_varchar_columns(con)

    quoted_values = []
    for value in config.FORBIDDEN_SENTINELS:
        quoted_values.append(f"'{value}'")
    values_sql = ", ".join(quoted_values)

    count_parts = []
    for col in varchar_cols:
        count_parts.append(f'count(*) FILTER (WHERE "{col}" IN ({values_sql})) AS "{col}"')
    sql = "SELECT " + ", ".join(count_parts) + " FROM clean"
    counts = query_one_row(con, sql)

    found = {}
    for i in range(len(varchar_cols)):
        if counts[i] > 0:
            found[varchar_cols[i]] = counts[i]

    if found:
        found_texts = []
        for col, n in found.items():
            found_texts.append(f"{col}={n:,}")
        detail = "남아 있는 결측 센티널: " + ", ".join(found_texts)
    else:
        detail = "남아 있는 결측 센티널: 없음"

    passed = (len(found) == 0)
    return make_result("no_forbidden_sentinels", passed, detail, {"found": found})


def check_numeric_types(con):
    """숫자 컬럼이 실제로 DOUBLE 타입인지."""
    column_types = {}
    for row in con.sql("DESCRIBE clean").fetchall():
        column_types[row[0]] = row[1]

    wrong = {}
    for col in config.NUMERIC_COLS:
        actual_type = column_types.get(col, "MISSING")
        if actual_type != "DOUBLE":
            wrong[col] = actual_type

    passed = (len(wrong) == 0)
    if passed:
        detail = "모든 숫자형 컬럼 DOUBLE"
    else:
        detail = f"타입 불일치: {wrong}"
    return make_result("numeric_types", passed, detail, {"wrong": wrong})


def check_unexpected_nulls(con, expected_inhome_nulls):
    """TRY_CAST 때문에 몰래 생긴 NULL 이 없는지.

    TRY_CAST 는 숫자로 못 바꾸는 값('N/A' 같은 것)을 만나면 에러 대신 NULL 을 만든다.
    그래서 새 데이터에 이상한 값이 섞여 오면 경고 없이 NULL 이 늘어난다.
      - NULL 을 허용하지 않는 숫자 컬럼: NULL 이 0개여야 한다
      - INHOME_RATE: NULL 개수가 원본의 '알수없음' 개수와 정확히 같아야 한다
        (예전에는 "INHOME_RATE 는 NULL 괜찮음"으로만 처리해서,
         다른 이상한 값이 들어와도 '알수없음'과 구분이 안 됐다)
    """
    not_null_cols = []
    for col in config.NUMERIC_COLS:
        if col not in config.NULLABLE_NUMERIC_COLS:
            not_null_cols.append(col)

    # 컬럼별 NULL 개수를 한 번에 센다
    count_parts = []
    for col in not_null_cols:
        count_parts.append(f'count(*) FILTER (WHERE "{col}" IS NULL) AS "{col}"')
    counts = query_one_row(con, "SELECT " + ", ".join(count_parts) + " FROM clean")

    found = {}
    for i in range(len(not_null_cols)):
        if counts[i] > 0:
            found[not_null_cols[i]] = counts[i]

    inhome_nulls = query_one_row(con, "SELECT count(*) FROM clean WHERE INHOME_RATE IS NULL")[0]
    extra_inhome_nulls = inhome_nulls - expected_inhome_nulls
    if extra_inhome_nulls != 0:
        found["INHOME_RATE"] = extra_inhome_nulls

    if found:
        detail = f"예상 밖 NULL: {found}"
    else:
        detail = "예상 밖 NULL 없음"
    detail += f" (INHOME_RATE NULL {inhome_nulls:,}건 / 원본 '알수없음' {expected_inhome_nulls:,}건)"

    passed = (len(found) == 0)
    metrics = {
        "found": found,
        "inhome_rate_nulls": inhome_nulls,
        "expected_inhome_nulls": expected_inhome_nulls,
    }
    return make_result("no_unexpected_nulls", passed, detail, metrics)


def check_ratio_range(con):
    """비율 컬럼이 0~100 사이인지 (CH_25_RATIO_MONTH1 을 100 으로 자른 게 적용됐는지)."""
    out_of_range = {}
    for col in config.RATIO_COLS:
        n = query_one_row(con, f'SELECT count(*) FROM clean WHERE "{col}" < 0 OR "{col}" > 100')[0]
        if n > 0:
            out_of_range[col] = n

    passed = (len(out_of_range) == 0)
    if passed:
        detail = "범위 이탈 없음"
    else:
        detail = f"범위 이탈: {out_of_range}"
    return make_result("ratio_range_0_100", passed, detail, {"out_of_range": out_of_range})


def check_target_values(con):
    """cancel_yn 이 '유지' 또는 '해지' 둘 중 하나이고 NULL 이 없는지."""
    quoted_values = []
    for value in config.VALID_TARGET_VALUES:
        quoted_values.append(f"'{value}'")
    values_sql = ", ".join(quoted_values)

    invalid = query_one_row(con, f"""
        SELECT count(*) FROM clean
        WHERE cancel_yn IS NULL OR cancel_yn NOT IN ({values_sql})
    """)[0]

    churn_rate = query_one_row(con, """
        SELECT avg(CAST(cancel_yn = '해지' AS INTEGER)) FROM clean
    """)[0]
    if churn_rate is None:   # clean 이 비어 있으면 avg 결과가 NULL(None)
        churn_rate = 0

    passed = (invalid == 0)
    detail = f"잘못된 타겟 값 {invalid:,}건, 해지 비율 {churn_rate:.2%}"
    metrics = {"invalid": invalid, "churn_rate": round(churn_rate, 6)}
    return make_result("target_values", passed, detail, metrics)


def check_label_fixed_per_customer(con):
    """달이 바뀌면서 cancel_yn 이 바뀐 고객(유지 -> 해지, 해지 -> 유지)이 있는지. (경고용)

    근거 (scripts/investigate_churn_rows.py): 실제 데이터에서는 이런 고객이 0명이다.
    cancel_yn 이 달마다 바뀌는 상태가 아니라 고객마다 고정된 라벨이라는 뜻이고,
    마트의 해석(첫 해지 행 = 첫 관측 달, 확실한 지표 = 해지 고객 비율)도 이 사실에 기대고 있다.

    바뀐 고객이 생기면 라벨 정의가 바뀌었을 수 있으니 확인이 필요하다.
    다만 재가입(해지 -> 유지)이나 해지(유지 -> 해지)는 현실에서 정상적인 일이라
    데이터가 틀렸다고 볼 수 없으므로 배포는 막지 않고 경고만 한다.
    """
    changed, keep_to_churn, churn_to_keep = query_one_row(con, """
        WITH label_by_month AS (
            SELECT
                sha2_hash,
                cancel_yn,
                lag(cancel_yn) OVER (PARTITION BY sha2_hash ORDER BY p_mt) AS prev_label
            FROM clean
        )
        SELECT
            count(DISTINCT sha2_hash) FILTER (WHERE cancel_yn <> prev_label),
            count(DISTINCT sha2_hash) FILTER (WHERE prev_label = '유지' AND cancel_yn = '해지'),
            count(DISTINCT sha2_hash) FILTER (WHERE prev_label = '해지' AND cancel_yn = '유지')
        FROM label_by_month
    """)

    passed = (changed == 0)
    detail = (f"라벨이 바뀐 고객 {changed:,}명 "
              f"(유지->해지 {keep_to_churn:,}명, 해지->유지 {churn_to_keep:,}명)")
    if not passed:
        detail += " → 라벨 정의가 바뀌었는지 확인 필요"
    metrics = {"changed_customers": changed,
               "keep_to_churn": keep_to_churn,
               "churn_to_keep": churn_to_keep}
    return make_result("label_fixed_per_customer", passed, detail, metrics, blocking=False)


def check_month_format(con):
    """p_mt 가 YYYYMM 형식인지 (예: 202302), 몇 개월치가 있는지."""
    # \d{4} = 숫자 4개(연도), (0[1-9]|1[0-2]) = 01~12(월)
    invalid = query_one_row(con, r"""
        SELECT count(*) FROM clean
        WHERE NOT regexp_full_match(p_mt, '\d{4}(0[1-9]|1[0-2])')
    """)[0]

    months = []
    for row in con.sql("SELECT DISTINCT p_mt FROM clean ORDER BY p_mt").fetchall():
        months.append(row[0])

    if months:
        first_month = months[0]
        last_month = months[-1]
    else:
        first_month = "-"
        last_month = "-"

    passed = (invalid == 0)
    detail = f"형식 오류 {invalid:,}건, 월 {len(months)}개 ({first_month} ~ {last_month})"
    return make_result("month_format", passed, detail, {"invalid": invalid, "months": months})


def run_clean_checks(con, raw_rows, drop_counts, expected_inhome_nulls):
    """정제 데이터 검사를 전부 실행하고 결과 리스트를 돌려준다."""
    results = []
    results.append(check_drop_reconciliation(con, raw_rows, drop_counts))
    results.append(check_primary_key(con))
    results.append(check_agmt_date_split(con))
    results.append(check_no_conflicted_customers(con))
    results.append(check_forbidden_sentinels(con))
    results.append(check_numeric_types(con))
    results.append(check_unexpected_nulls(con, expected_inhome_nulls))
    results.append(check_ratio_range(con))
    results.append(check_target_values(con))
    results.append(check_label_fixed_per_customer(con))
    results.append(check_month_format(con))
    return results


# =====================================================================
# 마트 검사
# =====================================================================

def check_mart_one_churn_per_customer(con):
    """churn_base 에서 고객마다 해지 줄이 최대 1개이고, 그게 그 고객의 마지막 달인지."""
    row = query_one_row(con, """
        WITH per_customer AS (
            SELECT
                sha2_hash,
                sum(is_churn)                          AS n_churn,
                max(p_mt)                              AS last_mt,
                max(p_mt) FILTER (WHERE is_churn = 1)  AS churn_mt
            FROM churn_base
            GROUP BY sha2_hash
        )
        SELECT
            count(*) FILTER (WHERE n_churn > 1)                        AS multi_churn,
            count(*) FILTER (WHERE n_churn = 1 AND churn_mt <> last_mt) AS rows_after_churn
        FROM per_customer
    """)
    multi_churn, rows_after_churn = row

    passed = (multi_churn == 0 and rows_after_churn == 0)
    detail = f"해지 행이 2개 이상인 고객 {multi_churn:,}명, 해지 이후 행이 남은 고객 {rows_after_churn:,}명"
    metrics = {"multi_churn": multi_churn, "rows_after_churn": rows_after_churn}
    return make_result("mart_one_churn_per_customer", passed, detail, metrics)


def check_mart_no_customer_lost(con):
    """해지 이후 줄을 자르면서 고객이 통째로 사라지지 않았는지 (줄은 줄어도 고객 수는 같아야 함)."""
    customers_clean = query_one_row(con, "SELECT count(DISTINCT sha2_hash) FROM clean")[0]
    customers_mart = query_one_row(con, "SELECT count(DISTINCT sha2_hash) FROM churn_base")[0]
    rows_clean = query_one_row(con, "SELECT count(*) FROM clean")[0]
    rows_mart = query_one_row(con, "SELECT count(*) FROM churn_base")[0]

    passed = (customers_clean == customers_mart)
    detail = (f"고객 {customers_clean:,} → {customers_mart:,}명, 행 {rows_clean:,} → {rows_mart:,} "
              f"(해지 이후 행 {rows_clean - rows_mart:,}개 제거)")
    metrics = {
        "customers_clean": customers_clean,
        "customers_mart": customers_mart,
        "rows_clean": rows_clean,
        "rows_mart": rows_mart,
    }
    return make_result("mart_no_customer_lost", passed, detail, metrics)


def check_mart_customer_one_row(con):
    """customer_churn 이 고객 1명 = 1행이고, 고객 수와 해지 고객 수가 정제 데이터와 같은지.

    정제 데이터에서 직접 센 값과 비교한다.
      - 고객 수      : clean 의 서로 다른 sha2_hash 수
      - 해지 고객 수 : clean 에서 '해지' 행이 하나라도 있는 고객 수
    """
    customers_clean, churners_clean = query_one_row(con, """
        SELECT
            count(DISTINCT sha2_hash),
            count(DISTINCT sha2_hash) FILTER (WHERE cancel_yn = '해지')
        FROM clean
    """)
    rows_mart, customers_mart, churners_mart = query_one_row(con, """
        SELECT count(*), count(DISTINCT sha2_hash), sum(is_churner)
        FROM customer_churn
    """)

    passed = (rows_mart == customers_mart == customers_clean and churners_mart == churners_clean)
    if rows_mart > 0:
        churn_pct = 100 * churners_mart / rows_mart
    else:
        churn_pct = 0.0
    detail = (f"고객 {customers_clean:,}명 → {rows_mart:,}행, "
              f"해지 고객 {churners_clean:,} → {churners_mart:,}명 ({churn_pct:.2f}%)")
    metrics = {
        "customers_clean": customers_clean,
        "rows_mart": rows_mart,
        "customers_mart": customers_mart,
        "churners_clean": churners_clean,
        "churners_mart": churners_mart,
    }
    return make_result("mart_customer_one_row", passed, detail, metrics)


def check_mart_segment_totals(con):
    """그룹별 고객 수를 다 더하면 customer_churn 전체 고객 수와 같은지 (집계에서 빠지거나 두 번 센 게 없는지)."""
    total = query_one_row(con, "SELECT count(*) FROM customer_churn")[0]
    rows = con.sql("""
        SELECT segment_col, sum(customers)
        FROM segment_churn
        GROUP BY segment_col
    """).fetchall()

    mismatch = {}
    for segment_col, segment_total in rows:
        if segment_total != total:
            mismatch[segment_col] = segment_total

    passed = (len(mismatch) == 0)
    if passed:
        detail = f"세그먼트 {len(rows)}개 모두 합계 {total:,}명과 일치"
    else:
        detail = f"합계 불일치: {mismatch}"
    return make_result("mart_segment_totals", passed, detail, {"mismatch": mismatch})


def run_mart_checks(con):
    """마트 검사를 전부 실행하고 결과 리스트를 돌려준다."""
    results = []
    results.append(check_mart_one_churn_per_customer(con))
    results.append(check_mart_no_customer_lost(con))
    results.append(check_mart_customer_one_row(con))
    results.append(check_mart_segment_totals(con))
    return results
