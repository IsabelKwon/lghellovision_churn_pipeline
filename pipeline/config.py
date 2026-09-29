"""파이프라인에서 쓰는 설정값 모음

경로나 기준값을 바꾸고 싶으면 이 파일을 통해서 고쳐야 합니다.
"""
from pathlib import Path

# 프로젝트 최상위 폴더 (이 파일이 pipeline/ 안에 있으니까 두 단계 위)
PROJECT_ROOT = Path(__file__).resolve().parent.parent


# ===== 경로 =====

# 원본 CSV 4개 (분기별)
RAW_GLOB = str(PROJECT_ROOT / "data" / "raw" / "*.csv")

# 정제 결과. 월(p_mt)별로 폴더가 나뉜 Parquet 파일
#   예: data/clean/churn_clean/p_mt=202302/part1_0.parquet
CLEAN_DIR = PROJECT_ROOT / "data" / "clean" / "churn_clean"

# 마트 (해지 여부 분석용으로 가공한 테이블)
#   churn_base/             고객별로 첫 해지 달까지만 남긴 테이블 (월별 행, 모델링용)
#   customer_churn.parquet  고객 1명 = 1행 (해지 고객 여부, 처음/마지막 관측 달)
#   segment_churn.parquet   그룹별 해지 고객 비율
MART_DIR = PROJECT_ROOT / "data" / "mart"

# 로그 파일과 품질검사 결과(JSON)를 저장하는 폴더
LOG_DIR = PROJECT_ROOT / "logs"

# SQL 파일 폴더
SQL_DIR = PROJECT_ROOT / "sql"


# ===== 해지율을 볼 그룹 컬럼 =====
# 여기에 컬럼을 추가하면 customer_churn, segment_churn 테이블에 자동으로 들어간다.
# 없는 컬럼 이름(오타 등)을 넣으면 run.py 의 check_segment_columns 가 시작 전에 멈춘다.
SEGMENT_COLS = [
    "AGMT_END_SEG",               # 약정 만료 시점
    "VOC_STOP_CANCEL_MONTH1_YN",  # 해지/정지 관련 상담(VOC) 여부
    "BUNDLE_YN",                  # 결합 상품 여부
    "SVC_USE_DAYS_GRP",           # 가입 기간
    "AGE_GRP10",                  # 연령대
]


# ===== DuckDB 설정 =====
# 노트북(RAM 16GB)에서 메모리 부족, 디스크 부족이 났어서 제한을 걸었다.
DUCKDB_CONFIG = {
    "memory_limit": "4GB",
    "threads": "2",
}
MAX_TEMP_DIR_SIZE = "2GiB"


# ===== 원본 CSV에 꼭 있어야 하는 컬럼 39개 =====
# 실제 CSV 파일을 열어보고 확인한 이름이다.
# 데이터와 같이 받은 설명 문서는 버전이 달라서 컬럼 이름이 조금 다르다.
#   예: 문서 PAID_CHNL_SCRB_CNT_GRP / CH_FAV_RANK1  <->  실제 PAID_CHNL_CNT_GRP / CH_FAV_RNK1
# 그래서 실행 전에 실제 파일의 컬럼을 먼저 확인한다. (run.py 의 check_schema)
EXPECTED_COLUMNS = [
    "sha2_hash", "SVC_USE_DAYS_GRP", "MEDIA_NM_GRP", "PROD_NM_GRP", "PROD_OLD_YN",
    "PROD_ONE_PLUS_YN", "AGMT_KIND_NM", "STB_RES_1M_YN", "SVOD_SCRB_CNT_GRP",
    "PAID_CHNL_CNT_GRP", "SCRB_PATH_NM_GRP", "INHOME_RATE", "AGMT_END_SEG", "AGMT_END_YMD",
    "TOTAL_USED_DAYS", "TV_SCRB", "ANALOG_SCRB", "DIGITAL_SCRB", "TOTAL_INTERNET_SCRB",
    "GIGA_INTERNET_SCRB", "BUNDLE_YN", "DIGITAL_GIGA_YN", "DIGITAL_ALOG_YN", "TV_I_CNT",
    "CH_LAST_DAYS_BF_GRP", "VOC_TOTAL_MONTH1_YN", "VOC_STOP_CANCEL_MONTH1_YN", "AGE_GRP10",
    "EMAIL_RECV_CLS_NM", "SMS_SEND_CLS_NM", "CH_HH_AVG_MONTH1", "CH_25_RATIO_MONTH1",
    "CH_25_RATIO_MEAN_3MM", "CH_FAV_RNK1", "KIDS_USE_PV_MONTH1", "NFX_USE_YN", "YTB_USE_YN",
    "p_mt", "cancel_yn",
]

# 03_clean.sql 에서 새로 만드는 컬럼 (R9). 원본에는 없지만 정제 데이터부터는 있다.
ADDED_CLEAN_COLUMNS = ["AGMT_END_DATE", "IS_NO_AGMT"]


# ===== 품질검사 기준 =====

# 전체에서 제외되는 줄의 비율이 이것보다 크면 실패
# (실제 실행 결과는 약 3.7%: 22,893,471줄 -> 22,053,564줄)
MAX_DROP_RATE = 0.10

# 규칙별로 제외 비율 상한을 따로 둔다. (이름은 02_drop_rules.sql 의 사유 이름과 같아야 함)
# 전체 비율만 보면 R1 이 갑자기 수만 줄을 빼도 10% 아래라서 눈치채지 못한다.
# 실제 실행 결과: R1 33줄, R2 6줄 (거의 0%), R3a 약 3.7%, R3b/R3c 0줄
# 이 숫자들은 "지금 데이터에서 정상인 범위"에 여유를 둔 값이라, 들어오는 데이터가 바뀌면 조정이 필요하다.
#   상한에 걸리면 배포가 멈춘다 -> 원인 확인
#     - 원천 데이터 오류면: 원천 담당자에게 알리고 고칠 때까지 기다린다
#     - 정상적인 변화면: 여기 숫자를 새 기준으로 고치고 다시 실행한다
# 데이터가 아주 작으면 한두 명만으로도 비율이 커져서 상한에 걸릴 수 있다. (테스트에서 실제로 겪음)
RULE_MAX_DROP_RATE = {
    "R1_corrupted_row":    0.001,  # 0.1%
    "R2_age_under_10":     0.001,  # 0.1%
    "R3a_label_conflict":  0.05,   # 5%
    # R3b, R3c 는 지금 데이터에 0건이라 낮게 잡았다. 갑자기 많아지면 원본 데이터가 바뀐 것.
    "R3b_exact_duplicate": 0.001,  # 0.1%
    "R3c_value_mismatch":  0.001,  # 0.1%
}

# 숫자로 변환하는 컬럼 (정제 후 DOUBLE 이어야 함)
NUMERIC_COLS = [
    "TOTAL_USED_DAYS", "TV_SCRB", "ANALOG_SCRB", "DIGITAL_SCRB",
    "TOTAL_INTERNET_SCRB", "GIGA_INTERNET_SCRB", "TV_I_CNT",
    "CH_HH_AVG_MONTH1", "CH_25_RATIO_MONTH1", "CH_25_RATIO_MEAN_3MM",
    "KIDS_USE_PV_MONTH1", "INHOME_RATE",
]

# 숫자 컬럼 중 NULL 이 있어도 되는 컬럼 ('알수없음'을 NULL 로 바꿨기 때문)
NULLABLE_NUMERIC_COLS = ["INHOME_RATE"]

# 정제 후에는 어느 컬럼에도 남아 있으면 안 되는 값 (빈 값을 뜻하는 표시들)
# '기타', '연령없음', '무약정', '3개월내없음'은 진짜 값이라서 여기 없다.
FORBIDDEN_SENTINELS = ['""', "정보없음", "알수없음", "10대미만"]

# 0~100 사이여야 하는 비율 컬럼
RATIO_COLS = ["CH_25_RATIO_MONTH1", "CH_25_RATIO_MEAN_3MM", "INHOME_RATE"]

# cancel_yn 에 들어갈 수 있는 값
VALID_TARGET_VALUES = ["유지", "해지"]
