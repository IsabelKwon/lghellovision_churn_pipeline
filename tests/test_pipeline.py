"""파이프라인 전체를 샘플 데이터로 돌려서 확인하는 테스트.

    python -m pytest            # 전체 테스트
    python -m pytest -v         # 테스트 이름까지 보기

확인하는 것
  1. 정상 데이터는 끝까지 성공한다
  2. 이상한 값이 들어오면 품질검사가 잡고 배포를 멈춘다
  3. 배포를 멈추면 기존 정상 데이터가 그대로 남는다
  4. 컬럼 이름이 틀리면 정제를 시작하기 전에 멈춘다
  5. 세그먼트 설정에 오타가 있으면 시작하기 전에 멈춘다
  6. 중복 3종류(라벨 충돌, 완전 중복, 값 불일치)를 각각 다른 사유로 처리한다
  7. 비율 컬럼의 빈 값이 100 으로 바뀌지 않고 품질검사에 걸린다 (LEAST 버그 재발 방지)
  (1번 안에서) 라벨이 달마다 바뀌는 샘플은 경고만 나고 배포는 계속된다
"""
from collections import Counter

import duckdb

from conftest import find_check, make_sample_data
from pipeline import config


def count_parquet_rows(folder):
    """폴더 아래 Parquet 파일들의 전체 줄 수."""
    pattern = f"{folder.as_posix()}/*/*.parquet"
    return duckdb.sql(f"SELECT count(*) FROM read_parquet('{pattern}', hive_partitioning = false)").fetchone()[0]


def normal_rows_without_duplicates(files):
    """첫 번째 파일에서, 중복/오염과 관계없는 '유지' 줄만 골라준다.

    테스트용으로 줄을 복사하거나 값을 바꿀 때, 이미 충돌 고객인 줄을 고르면
    R3a 가 먼저 적용돼서 원하는 결과를 확인할 수 없기 때문이다.
    """
    first_file = files["sha_tps_cancel_202302_to_202304.csv"]

    all_rows = []
    for rows in files.values():
        all_rows.extend(rows)
    key_counts = Counter((row["sha2_hash"], row["p_mt"]) for row in all_rows)

    customers_with_dup = set()
    for (customer, month), count in key_counts.items():
        if count > 1:
            customers_with_dup.add(customer)

    result = []
    for row in first_file:
        if row["sha2_hash"] in customers_with_dup:
            continue
        if row["STB_RES_1M_YN"] not in ("Y", "N"):   # 오염 행(R1), 빈 값 행 제외
            continue
        if row["AGE_GRP10"] == "10대미만":            # R2 제외
            continue
        if row["cancel_yn"] != "유지":
            continue
        result.append(row)
    return result


# ---------------------------------------------------------------------

def test_normal_data_passes(pipeline_env, sample_files):
    exit_code, report = pipeline_env["run"](sample_files)

    assert exit_code == 0
    assert report["passed"] is True
    for check in report["clean_checks"] + report["mart_checks"]:
        if check["blocking"]:
            assert check["passed"], f"{check['name']} 실패: {check['detail']}"

    # 샘플 데이터는 '유지였다가 해지'로 바뀌는 고객을 만들기 때문에 경고가 나야 한다.
    # 경고는 배포를 막지 않으므로 위에서 exit_code == 0 이다.
    label_check = find_check(report, "label_fixed_per_customer")
    assert label_check["blocking"] is False
    assert label_check["passed"] is False
    assert label_check["metrics"]["keep_to_churn"] > 0

    # 원본 = 정제 + 제외 (행 대사)
    reconciliation = find_check(report, "drop_reconciliation")
    assert reconciliation["metrics"]["balanced"] is True

    # 결과 파일이 실제로 배포됐는지
    assert count_parquet_rows(pipeline_env["clean_dir"]) == reconciliation["metrics"]["clean_rows"]
    assert (pipeline_env["mart_dir"] / "segment_churn.parquet").exists()
    assert (pipeline_env["mart_dir"] / "customer_churn.parquet").exists()

    # customer_churn: 고객 1명 = 1행, 해지 고객 수가 정제 데이터에서 직접 센 값과 같음
    customer_check = find_check(report, "mart_customer_one_row")
    assert customer_check["passed"] is True
    assert customer_check["metrics"]["churners_mart"] > 0


def test_bad_numeric_value_stops_deploy(pipeline_env):
    # 숫자 컬럼(TOTAL_USED_DAYS)에 'N/A' 가 4건 섞인 데이터
    bad_files = make_sample_data.generate(n_customers=1000, seed=42, inject_bad=True)

    exit_code, report = pipeline_env["run"](bad_files)

    assert exit_code == 1
    null_check = find_check(report, "no_unexpected_nulls")
    assert null_check["passed"] is False
    assert "TOTAL_USED_DAYS" in null_check["metrics"]["found"]
    # 배포되지 않았어야 한다
    assert not pipeline_env["clean_dir"].exists()


def test_existing_data_kept_when_check_fails(pipeline_env, sample_files):
    # 1) 정상 데이터로 한 번 배포
    exit_code, _ = pipeline_env["run"](sample_files)
    assert exit_code == 0
    rows_before = count_parquet_rows(pipeline_env["clean_dir"])

    # 2) 불량 데이터로 다시 실행 -> 실패
    bad_files = make_sample_data.generate(n_customers=500, seed=7, inject_bad=True)
    exit_code, _ = pipeline_env["run"](bad_files)
    assert exit_code == 1

    # 3) 기존 정상 데이터가 그대로 남아 있어야 한다
    assert count_parquet_rows(pipeline_env["clean_dir"]) == rows_before


def test_wrong_column_names_stop_before_cleaning(pipeline_env, sample_files):
    # 설명 문서 버전의 컬럼 이름 (CH_FAV_RANK1 등) 으로 저장된 파일
    exit_code, report = pipeline_env["run"](sample_files, use_spec_names=True)

    assert exit_code == 1
    for file_result in report["schema"].values():
        assert "CH_FAV_RNK1" in file_result["missing"]
    # 정제를 시작하지 않았으니 품질검사 결과도 없다
    assert "clean_checks" not in report
    assert not pipeline_env["clean_dir"].parent.exists()


def test_segment_typo_stops_before_cleaning(pipeline_env, sample_files, monkeypatch):
    monkeypatch.setattr(config, "SEGMENT_COLS", ["AGMT_END_SEG", "BUNDEL_YN"])   # BUNDLE_YN 오타

    exit_code, report = pipeline_env["run"](sample_files)

    assert exit_code == 1
    assert report["segment_cols_missing"] == ["BUNDEL_YN"]
    assert "clean_checks" not in report


def test_duplicate_types_get_different_reasons(pipeline_env):
    # 고객 3,000명으로 만든다.
    # 1,000명이면 값 불일치 고객 1명의 줄(약 12줄)이 전체의 0.1% 를 넘어서
    # config 의 R3c 상한에 걸려 실패한다. (상한 검사가 제대로 동작한다는 뜻이기도 함)
    files = make_sample_data.generate(n_customers=3000, seed=42, inject_bad=False)
    normal_rows = normal_rows_without_duplicates(files)
    first_file = files["sha_tps_cancel_202302_to_202304.csv"]

    # 완전 중복: 똑같은 줄을 한 번 더 넣는다
    exact_row = normal_rows[0]
    first_file.append(dict(exact_row))

    # 값 불일치: 라벨은 같고 TV_SCRB 만 다른 줄을 넣는다
    mismatch_row = normal_rows[1]
    first_file.append(dict(mismatch_row, TV_SCRB="99"))

    exit_code, report = pipeline_env["run"](files)

    assert exit_code == 0
    assert report["dup_customers_by_type"]["exact_duplicate"] == 1
    assert report["dup_customers_by_type"]["value_mismatch"] == 1
    assert report["dup_customers_by_type"]["label_conflict"] > 0   # 샘플에 원래 심어둔 충돌 고객

    dropped = report["dropped_by_rule"]
    assert dropped["R3b_exact_duplicate"] == 1   # 복사한 1줄만 빠지고 원래 줄은 남는다
    assert dropped["R3c_value_mismatch"] > 0     # 그 고객의 모든 달이 빠진다

    # 중복이 다 정리돼서 (고객, 달)이 유일해야 한다
    assert find_check(report, "primary_key_unique")["passed"] is True


def test_blank_ratio_is_not_turned_into_100(pipeline_env, sample_files):
    normal_rows = normal_rows_without_duplicates(sample_files)
    normal_rows[0]["CH_25_RATIO_MONTH1"] = ""      # 빈 값
    normal_rows[1]["CH_25_RATIO_MONTH1"] = "N/A"   # 숫자가 아닌 값

    exit_code, report = pipeline_env["run"](sample_files)

    # LEAST(값, 100) 을 쓰면 두 값이 조용히 100 이 되어 통과해 버린다.
    # 지금은 NULL 로 남아서 no_unexpected_nulls 가 잡아야 한다.
    assert exit_code == 1
    null_check = find_check(report, "no_unexpected_nulls")
    assert null_check["metrics"]["found"]["CH_25_RATIO_MONTH1"] == 2
