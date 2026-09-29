"""원본 CSV -> 정제 -> 품질검사 -> 배포 -> 마트 를 한 번에 실행하는 파이프라인.

순서
  0. 스키마 검사: 원본 CSV에 필요한 컬럼이 다 있는지 (헤더만 읽음)
  --- 정제 ---
  1. 중복/충돌 고객 찾기              (sql/01a_dup_keys.sql, 01b_dup_rows.sql, 01c_conflicted_customers.sql)
  2. 파일마다 제외 사유를 붙이고 -> 사유별 개수 세고 -> 정제해서 임시 폴더에 저장
                                       (sql/02_drop_rules.sql, sql/03_clean.sql)
  3. 임시 폴더의 결과를 품질검사        (pipeline/quality.py)
  4. 배포를 막는 검사를 전부 통과하면 최종 폴더로 옮김. 하나라도 실패하면 옮기지 않고 멈춤
  --- 마트 ---
  5. 정제 데이터로 마트 만들기          (sql/04_mart_churn_base.sql, sql/05_mart_customer_churn.sql,
                                       sql/06_mart_segment_churn.sql)
  6. 마트 품질검사 후 최종 폴더로 옮김

같은 원본으로 여러 번 돌려도 항상 같은 결과가 나온다.
"""
import difflib
import glob
import json
import logging
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

import duckdb

from pipeline import config
from pipeline import quality

log = logging.getLogger("churn_pipeline")
TOTAL_STEPS = 6


# =====================================================================
# 도우미 함수
# =====================================================================

def setup_logging(run_id):
    """로그를 화면과 파일에 동시에 찍도록 설정하고, 로그 파일 경로를 돌려준다."""
    config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = config.LOG_DIR / f"pipeline_{run_id}.log"

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S")
    screen_handler = logging.StreamHandler(sys.stdout)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    screen_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    log.setLevel(logging.INFO)
    log.handlers.clear()   # 같은 프로세스에서 두 번 실행해도 로그가 두 번 찍히지 않게
    log.addHandler(screen_handler)
    log.addHandler(file_handler)
    return log_path


def make_sql_file_list(paths):
    """파일 경로 리스트를 SQL 에 넣을 수 있는 문자열로 바꾼다.

    예: ["a.csv", "b.csv"]  ->  "['a.csv', 'b.csv']"
    경로에 작은따옴표(')가 있으면 SQL 문법이 깨지므로 '' 로 바꿔준다.
    """
    items = []
    for path in paths:
        safe_path = path.replace("'", "''")
        items.append(f"'{safe_path}'")
    return "[" + ", ".join(items) + "]"


def load_sql(file_name, **values):
    """sql/ 폴더의 파일을 읽고, {raw_files} 같은 자리에 값을 채워서 돌려준다."""
    sql_path = config.SQL_DIR / file_name
    sql_text = sql_path.read_text(encoding="utf-8")
    return sql_text.format(**values)


def connect_duckdb():
    """메모리/임시 디스크 제한을 건 DuckDB 연결을 만든다."""
    temp_dir = config.PROJECT_ROOT / "data" / "_duckdb_tmp"
    temp_dir.mkdir(parents=True, exist_ok=True)

    duckdb_settings = dict(config.DUCKDB_CONFIG)
    duckdb_settings["temp_directory"] = str(temp_dir)
    con = duckdb.connect(config=duckdb_settings)

    con.sql(f"SET max_temp_directory_size = '{config.MAX_TEMP_DIR_SIZE}'")
    con.sql("SET preserve_insertion_order = false")   # 큰 파일을 쓸 때 메모리를 덜 쓰게 함
    return con


def create_parquet_view(con, view_name, path, partitioned):
    """Parquet 파일을 SQL 에서 테이블처럼 쓸 수 있도록 뷰를 만든다.

    partitioned=True 이면 path 아래 p_mt=.../ 폴더들의 파일을 전부 읽는다.
    """
    if partitioned:
        pattern = f"{path.as_posix()}/*/*.parquet"
    else:
        pattern = path.as_posix()

    con.sql(f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT * FROM read_parquet('{pattern}', hive_partitioning = false)
    """)


def log_check_results(step, results):
    """품질검사 결과를 로그에 찍고, 배포를 막는 검사가 전부 통과했으면 True 를 돌려준다.

    경고용 검사(blocking=False)는 실패해도 WARN 으로 찍기만 하고 배포는 막지 않는다.
    """
    passed_count = 0
    blocking_failed = 0
    for result in results:
        if result["passed"]:
            status = "PASS"
            passed_count += 1
        elif result["blocking"]:
            status = "FAIL"
            blocking_failed += 1
        else:
            status = "WARN"
        log.info(f"[{step}/{TOTAL_STEPS}] {status} {result['name']:<28} {result['detail']}")

    log.info(f"[{step}/{TOTAL_STEPS}] 품질검사 {passed_count}/{len(results)} 통과")
    return blocking_failed == 0


def replace_folder(new_folder, target_folder):
    """검사를 통과한 임시 폴더로 최종 폴더를 바꿔치기한다."""
    if target_folder.exists():
        shutil.rmtree(target_folder)
    target_folder.parent.mkdir(parents=True, exist_ok=True)
    new_folder.rename(target_folder)


def save_report(report, run_id, start_time, passed):
    """실행 결과를 logs/quality_report_<run_id>.json 으로 저장한다."""
    report["passed"] = passed
    report["elapsed_sec"] = round(time.time() - start_time, 1)

    report_path = config.LOG_DIR / f"quality_report_{run_id}.json"
    report_text = json.dumps(report, ensure_ascii=False, indent=2)
    report_path.write_text(report_text, encoding="utf-8")
    return report_path


# =====================================================================
# 0단계: 스키마 검사
# =====================================================================

def check_schema(con, raw_files):
    """원본 파일마다 필요한 컬럼이 다 있는지 확인한다.

    컬럼이 빠지거나 이름이 바뀌면 SQL 에서 에러가 나거나, 더 나쁘면 틀린 결과가 조용히 나온다.
    오래 걸리는 정제를 시작하기 전에 헤더만 읽어서 빨리 확인한다.

    돌려주는 값 예시:
        {"파일이름.csv": {"missing": [빠진 컬럼들], "extra": [예상에 없던 컬럼들]}, ...}
    """
    expected = set(config.EXPECTED_COLUMNS)
    result = {}

    for file_path in raw_files:
        file_name = Path(file_path).name

        # DESCRIBE 는 데이터를 다 읽지 않고 컬럼 이름과 타입만 알려준다
        describe_rows = con.sql(
            f"DESCRIBE SELECT * FROM read_csv({make_sql_file_list([file_path])}, all_varchar = true)"
        ).fetchall()
        columns = []
        for row in describe_rows:
            columns.append(row[0])

        missing = sorted(expected - set(columns))
        extra = sorted(set(columns) - expected)
        result[file_name] = {"missing": missing, "extra": extra}

        if missing:
            # 빠진 컬럼과 이름이 비슷한 컬럼이 있으면 "혹시 이거?" 하고 알려준다
            # 예: CH_FAV_RNK1 이 없는데 CH_FAV_RANK1 이 있으면 -> "CH_FAV_RNK1 (혹시 CH_FAV_RANK1?)"
            hint_texts = []
            for col in missing:
                similar = difflib.get_close_matches(col, extra, n=1, cutoff=0.7)
                if similar:
                    hint_texts.append(f"{col} (혹시 {similar[0]}?)")
                else:
                    hint_texts.append(col)
            log.error(f"[0/{TOTAL_STEPS}] FAIL {file_name}: 필수 컬럼 없음 → {', '.join(hint_texts)}")
        elif extra:
            log.warning(f"[0/{TOTAL_STEPS}] WARN {file_name}: 예상 밖 컬럼 {extra} (그대로 통과됨)")
        else:
            log.info(f"[0/{TOTAL_STEPS}] PASS {file_name}: 컬럼 {len(columns)}개 일치")

    return result


def check_segment_columns():
    """config.SEGMENT_COLS 에 적은 컬럼이 실제로 있는 컬럼인지 확인한다.

    오타가 있으면 5단계(마트 만들기)에서야 SQL 에러가 난다.
    그때는 이미 정제 데이터가 배포된 뒤라 "정제는 새 데이터, 마트는 옛날 데이터"인 상태가 된다.
    그래서 스키마 검사처럼 시작하기 전에 확인한다.

    쓸 수 있는 컬럼 = 원본 컬럼(EXPECTED_COLUMNS) + 03_clean.sql 에서 새로 만든 컬럼(ADDED_CLEAN_COLUMNS)
    돌려주는 값: 없는 컬럼 이름 리스트 (문제없으면 빈 리스트)
    """
    available = config.EXPECTED_COLUMNS + config.ADDED_CLEAN_COLUMNS

    missing = []
    for col in config.SEGMENT_COLS:
        if col not in available:
            missing.append(col)

    if missing:
        # 스키마 검사처럼 이름이 비슷한 컬럼이 있으면 "혹시 이거?" 하고 알려준다
        hint_texts = []
        for col in missing:
            similar = difflib.get_close_matches(col, available, n=1, cutoff=0.7)
            if similar:
                hint_texts.append(f"{col} (혹시 {similar[0]}?)")
            else:
                hint_texts.append(col)
        log.error(f"[0/{TOTAL_STEPS}] FAIL config.SEGMENT_COLS 에 없는 컬럼 → {', '.join(hint_texts)}")
    else:
        log.info(f"[0/{TOTAL_STEPS}] PASS config.SEGMENT_COLS: 컬럼 {len(config.SEGMENT_COLS)}개 확인")

    return missing


# =====================================================================
# 1단계: 중복/충돌 고객 찾기
# =====================================================================

def find_conflicted_customers(con, raw_files, report):
    start = time.time()

    # (1) 중복된 (고객, 달) 찾기 - 파일 4개를 한꺼번에, 컬럼 2개만 읽음
    con.sql(load_sql("01a_dup_keys.sql", raw_files=make_sql_file_list(raw_files)))

    # (2) 중복 줄의 내용을 해시로 저장 - 메모리 부족을 피하려고 파일 1개씩
    for file_path in raw_files:
        con.sql(load_sql("01b_dup_rows.sql", raw_file=make_sql_file_list([file_path])))

    # (3) 중복을 3종류로 나누고 통째로 뺄 고객 결정
    con.sql(load_sql("01c_conflicted_customers.sql"))

    total_conflicted = con.sql("SELECT count(*) FROM conflicted_customers").fetchone()[0]

    # 중복 종류별 고객 수 (예: {"label_conflict": 40063})
    dup_types = {}
    rows = con.sql("""
        SELECT dup_type, count(DISTINCT sha2_hash)
        FROM dup_groups
        GROUP BY dup_type
    """).fetchall()
    for dup_type, customer_count in rows:
        dup_types[dup_type] = customer_count
    report["dup_customers_by_type"] = dup_types

    type_texts = []
    for dup_type in sorted(dup_types):
        type_texts.append(f"{dup_type} {dup_types[dup_type]:,}명")
    if type_texts:
        type_text = ", ".join(type_texts)
    else:
        type_text = "없음"

    log.info(f"[1/{TOTAL_STEPS}] 고객 전체 제외 {total_conflicted:,}명 | "
             f"중복 유형별 고객: {type_text} ({time.time() - start:.1f}s)")


# =====================================================================
# 2단계: 파일별 정제
# =====================================================================

def clean_raw_files(con, raw_files, staging_dir):
    """원본 파일을 하나씩 정제해서 staging_dir 에 Parquet 으로 저장한다.

    4개를 한꺼번에 처리하지 않고 하나씩 하는 이유: 메모리와 임시 디스크를 약 1/4 만 쓰기 위해.

    돌려주는 값
        raw_rows              : 원본 전체 줄 수
        drop_counts           : 사유별 제외 줄 수 (예: {"R1_corrupted_row": 33, ...})
        expected_inhome_nulls : 정제 후 INHOME_RATE 가 NULL 이어도 되는 줄 수 (= 원본 '알수없음' 개수)
    """
    raw_rows = 0
    drop_counts = {}
    expected_inhome_nulls = 0

    # 완전 중복(R3b)이 있을 때만 row_number() 로 줄 번호를 매긴다.
    # row_number() 는 무거워서, 실제 데이터(완전 중복 0건)에서 2단계를 약 2배 느리게 만들었다.
    # 완전 중복이 없으면 번호를 1로 고정해도 결과가 같다. (이유는 02_drop_rules.sql 주석 참고)
    n_exact_dup = con.sql("SELECT count(*) FROM dup_groups WHERE dup_type = 'exact_duplicate'").fetchone()[0]
    if n_exact_dup > 0:
        row_num_sql = "row_number() OVER (PARTITION BY sha2_hash, p_mt)"
    else:
        row_num_sql = "1"

    file_number = 0
    for file_path in raw_files:
        file_number += 1
        file_name = Path(file_path).name

        # (1) 모든 줄에 제외 사유를 붙인 뷰 raw_tagged 만들기
        con.sql(load_sql("02_drop_rules.sql",
                         raw_file=make_sql_file_list([file_path]),
                         row_num_sql=row_num_sql))

        # (2) 사유별 개수 세기. 정제할 때도 같은 뷰를 쓰니까 개수가 어긋날 수 없다
        rows = con.sql("""
            SELECT
                _drop_reason,
                count(*) AS n_rows,
                count(*) FILTER (WHERE INHOME_RATE = '알수없음') AS n_inhome_unknown
            FROM raw_tagged
            GROUP BY _drop_reason
        """).fetchall()

        file_rows = 0
        file_drops = {}
        for drop_reason, n_rows, n_inhome_unknown in rows:
            file_rows += n_rows
            if drop_reason is None:
                # 남는 줄 중에서 INHOME_RATE 가 '알수없음'인 것만 정제 후 NULL 이 된다
                expected_inhome_nulls += n_inhome_unknown
            else:
                file_drops[drop_reason] = n_rows
                drop_counts[drop_reason] = drop_counts.get(drop_reason, 0) + n_rows
        raw_rows += file_rows

        # (3) 정제해서 월(p_mt)별 폴더로 나눠 저장
        #     FILENAME_PATTERN 의 {i} 는 DuckDB 가 0, 1, 2... 로 채운다.
        #     파이썬 f-string 안이라서 {{i}} 로 써야 글자 그대로 {i} 가 들어간다.
        #     part{file_number} 를 앞에 붙여서 파일끼리 이름이 겹치지 않게 한다.
        clean_sql = load_sql("03_clean.sql")
        con.sql(f"""
            COPY ({clean_sql}) TO '{staging_dir.as_posix()}'
            (FORMAT PARQUET,
             PARTITION_BY (p_mt),
             WRITE_PARTITION_COLUMNS true,
             OVERWRITE_OR_IGNORE true,
             FILENAME_PATTERN 'part{file_number}_{{i}}')
        """)

        drop_texts = []
        for reason in sorted(file_drops):
            drop_texts.append(f"{reason} {file_drops[reason]:,}")
        if drop_texts:
            drop_text = ", ".join(drop_texts)
        else:
            drop_text = "없음"
        log.info(f"[2/{TOTAL_STEPS}] {file_name}: 원본 {file_rows:,}행 → 제외 {drop_text}")

    return raw_rows, drop_counts, expected_inhome_nulls


# =====================================================================
# 5단계: 마트 만들기
# =====================================================================

def make_customer_segment_cols():
    """05_mart_customer_churn.sql 의 {segment_cols} 자리에 들어갈 SQL 을 만든다.

    예: SEGMENT_COLS = ["AGMT_END_SEG", "BUNDLE_YN"] 이면
        c."AGMT_END_SEG",
        c."BUNDLE_YN"
    """
    cols = []
    for col in config.SEGMENT_COLS:
        cols.append(f'    c."{col}"')
    return ",\n".join(cols)


def make_segment_unions():
    """06_mart_segment_churn.sql 의 {segment_unions} 자리에 들어갈 SQL 을 만든다.

    SEGMENT_COLS 의 컬럼마다 SELECT 를 하나씩 만들어서 UNION ALL 로 붙인다.
    segment_value 는 컬럼마다 타입이 다를 수 있어서 전부 문자열(VARCHAR)로 맞춘다.
    고객 1명 = 1행인 customer_churn 으로 세기 때문에 count(*) 가 곧 고객 수다.
    """
    selects = []
    for col in config.SEGMENT_COLS:
        sql = (
            f"    SELECT '{col}' AS segment_col,\n"
            f'           CAST("{col}" AS VARCHAR) AS segment_value,\n'
            f"           count(*) AS customers,\n"
            f"           CAST(sum(is_churner) AS BIGINT) AS churners\n"
            f"    FROM customer_churn\n"
            f'    GROUP BY "{col}"'
        )
        selects.append(sql)
    return "\n    UNION ALL\n".join(selects)


def build_marts(con, mart_staging):
    start = time.time()

    # churn_base: 고객별 첫 해지 달까지만 남긴 테이블 (월별 폴더로 저장)
    base_dir = mart_staging / "churn_base"
    base_dir.mkdir(parents=True, exist_ok=True)
    base_sql = load_sql("04_mart_churn_base.sql")
    con.sql(f"""
        COPY ({base_sql}) TO '{base_dir.as_posix()}'
        (FORMAT PARQUET, PARTITION_BY (p_mt), WRITE_PARTITION_COLUMNS true, OVERWRITE_OR_IGNORE true)
    """)
    create_parquet_view(con, "churn_base", base_dir, partitioned=True)

    # customer_churn: 고객 1명 = 1행 (파일 1개)
    customer_path = mart_staging / "customer_churn.parquet"
    customer_sql = load_sql("05_mart_customer_churn.sql", segment_cols=make_customer_segment_cols())
    con.sql(f"COPY ({customer_sql}) TO '{customer_path.as_posix()}' (FORMAT PARQUET)")
    create_parquet_view(con, "customer_churn", customer_path, partitioned=False)

    # segment_churn: 그룹별 해지 고객 비율 (파일 1개). customer_churn 으로 계산한다.
    seg_path = mart_staging / "segment_churn.parquet"
    seg_sql = load_sql("06_mart_segment_churn.sql", segment_unions=make_segment_unions())
    con.sql(f"COPY ({seg_sql}) TO '{seg_path.as_posix()}' (FORMAT PARQUET)")
    create_parquet_view(con, "segment_churn", seg_path, partitioned=False)

    log.info(f"[5/{TOTAL_STEPS}] 마트 생성 완료: churn_base, customer_churn, segment_churn "
             f"({time.time() - start:.1f}s)")


# =====================================================================
# 전체 실행
# =====================================================================

def run(raw_glob=config.RAW_GLOB):
    """파이프라인 전체를 실행한다. 성공하면 0, 실패하면 1 을 돌려준다."""
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = setup_logging(run_id)
    start_time = time.time()
    report = {"run_id": run_id}   # 마지막에 JSON 으로 저장할 실행 결과

    # 원본 파일 목록
    #   glob.glob("data/raw/*.csv") -> 패턴(*.csv)에 맞는 실제 파일 경로들을 리스트로 돌려준다
    #   sorted() -> glob 이 돌려주는 순서는 보장되지 않아서 이름순으로 고정한다
    #               (파일 이름에 기간이 있어서 이름순 = 시간순. 매번 같은 순서로 처리해야 결과 파일 이름도 같다)
    raw_files = sorted(glob.glob(raw_glob))
    if len(raw_files) == 0:
        log.error(f"원본 CSV를 찾지 못했습니다: {raw_glob}")
        return 1

    raw_file_names = []
    for f in raw_files:
        raw_file_names.append(Path(f).name)
    report["raw_files"] = raw_file_names

    log.info(f"run_id={run_id}, 원본 파일 {len(raw_files)}개")
    for name in raw_file_names:
        log.info(f"  - {name}")

    con = connect_duckdb()

    # ---------- 0. 스키마 검사 ----------
    schema = check_schema(con, raw_files)
    report["schema"] = schema

    segment_missing = check_segment_columns()
    report["segment_cols_missing"] = segment_missing

    schema_ok = True
    for file_result in schema.values():
        if file_result["missing"]:
            schema_ok = False
    if segment_missing:
        schema_ok = False
    if not schema_ok:
        con.close()
        save_report(report, run_id, start_time, False)
        log.error(f"[0/{TOTAL_STEPS}] 스키마 불일치 → 정제를 시작하지 않고 중단합니다.")
        return 1

    # ---------- 1. 중복/충돌 고객 ----------
    find_conflicted_customers(con, raw_files, report)

    # ---------- 2. 파일별 정제 -> 임시 폴더 ----------
    clean_staging = config.CLEAN_DIR.parent / f"_staging_{run_id}"
    clean_staging.mkdir(parents=True, exist_ok=True)

    step_start = time.time()
    raw_rows, drop_counts, expected_inhome_nulls = clean_raw_files(con, raw_files, clean_staging)
    report["dropped_by_rule"] = drop_counts
    log.info(f"[2/{TOTAL_STEPS}] 정제 완료 ({time.time() - step_start:.1f}s)")

    # ---------- 3. 정제 결과 품질검사 ----------
    create_parquet_view(con, "clean", clean_staging, partitioned=True)
    clean_results = quality.run_clean_checks(con, raw_rows, drop_counts, expected_inhome_nulls)
    report["clean_checks"] = clean_results

    clean_passed = log_check_results(3, clean_results)
    if not clean_passed:
        con.close()
        save_report(report, run_id, start_time, False)
        log.error(f"[4/{TOTAL_STEPS}] 정제 품질검사 실패 → 배포 중단. 기존 {config.CLEAN_DIR} 는 그대로 유지됩니다.")
        log.error(f"      실패한 결과는 확인용으로 {clean_staging} 에 남겨둡니다.")
        return 1

    # ---------- 4. 정제 데이터 배포 ----------
    replace_folder(clean_staging, config.CLEAN_DIR)
    create_parquet_view(con, "clean", config.CLEAN_DIR, partitioned=True)   # 옮긴 위치로 뷰 다시 만들기
    log.info(f"[4/{TOTAL_STEPS}] 정제 데이터 배포 완료 → {config.CLEAN_DIR}")

    # ---------- 5. 마트 만들기 -> 임시 폴더 ----------
    mart_staging = config.MART_DIR.parent / f"_mart_staging_{run_id}"
    build_marts(con, mart_staging)

    # ---------- 6. 마트 품질검사 -> 배포 ----------
    mart_results = quality.run_mart_checks(con)
    report["mart_checks"] = mart_results
    mart_passed = log_check_results(6, mart_results)
    con.close()
    report_path = save_report(report, run_id, start_time, mart_passed)

    if not mart_passed:
        log.error(f"[6/{TOTAL_STEPS}] 마트 품질검사 실패 → 마트 배포 중단 (정제 데이터는 이미 배포됨). "
                  f"확인용: {mart_staging}")
        return 1

    replace_folder(mart_staging, config.MART_DIR)
    log.info(f"[6/{TOTAL_STEPS}] 마트 배포 완료 → {config.MART_DIR}")
    log.info(f"총 소요 {time.time() - start_time:.1f}s | 로그 {log_path.name} | 리포트 {report_path.name}")
    return 0
