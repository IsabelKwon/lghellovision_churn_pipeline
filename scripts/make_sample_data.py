"""원본과 같은 구조(39개 컬럼, 분기별 CSV 4개)의 가짜 샘플 데이터를 만든다.

실제 LG헬로비전 데이터는 외부 공개가 불가능하므로, 저장소를 받은 사람도
파이프라인을 직접 돌려볼 수 있도록 원본에서 발견한 문제들을 그대로 심어둔 샘플을 제공한다.

    python scripts/make_sample_data.py              # 정상 샘플
    python scripts/make_sample_data.py --inject-bad # 품질검사 실패 시연용
    python scripts/make_sample_data.py --spec-names # 스키마 검사 실패 시연용

심어둔 문제 (실제 데이터에서 발견된 것과 동일한 유형)
  - STB_RES_1M_YN='""' 등 여러 컬럼이 동시에 센티널인 오염 행
  - STB_RES_1M_YN 이 비어 있는(NULL) 정상 행 (제외되면 안 됨)
  - AGE_GRP10='10대미만' 행
  - 같은 고객·같은 달에 유지/해지가 동시에 있는 충돌 고객 (이후 달도 계속 충돌)
  - AGMT_KIND_NM / AGMT_END_YMD / SCRB_PATH_NM_GRP = '정보없음'
  - INHOME_RATE = '알수없음'
  - CH_25_RATIO_MONTH1 > 100
"""
import argparse
import csv
import hashlib
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "data" / "raw"

sys.path.insert(0, str(ROOT))
from pipeline.config import EXPECTED_COLUMNS as COLUMNS  # noqa: E402

# 샘플 파일(20241204)에 적힌 컬럼 이름 → 스키마 검사 실패 시연용
SPEC_NAMES = {"PAID_CHNL_CNT_GRP": "PAID_CHNL_SCRB_CNT_GRP", "CH_FAV_RNK1": "CH_FAV_RANK1"}
MONTHS = [f"2023{m:02d}" for m in range(2, 13)]
QUARTER_FILES = {
    "sha_tps_cancel_202302_to_202304.csv": MONTHS[0:3],
    "sha_tps_cancel_202305_to_202307.csv": MONTHS[3:6],
    "sha_tps_cancel_202308_to_202310.csv": MONTHS[6:9],
    "sha_tps_cancel_202311_to_202312.csv": MONTHS[9:11],
}
YN = ["Y", "N"]


def customer_profile(rng: random.Random, i: int) -> dict:
    tv = rng.choice([1, 1, 1, 2, 3])
    return {
        "sha2_hash": hashlib.sha256(f"customer-{i}".encode()).hexdigest(),
        "SVC_USE_DAYS_GRP": rng.choice(["6개월미만", "6개월~12개월미만", "12개월~24개월미만", "24개월~ 36개월미만", "36개월 이상"]),
        "MEDIA_NM_GRP": rng.choice(["HD", "UHD", "UHD", "기타"]),
        "PROD_NM_GRP": rng.choice(["이코노미", "베이직", "스탠다드", "프리미엄", "라이트"]),
        "PROD_OLD_YN": rng.choice(YN), "PROD_ONE_PLUS_YN": rng.choice(YN),
        "AGMT_KIND_NM": rng.choice(["신규", "재약정", "약정승계", "무약정", "정보없음"]),
        "STB_RES_1M_YN": rng.choice(YN),
        "SVOD_SCRB_CNT_GRP": rng.choice(["0건", "1건", "2건", "3건 이상"]),
        "PAID_CHNL_CNT_GRP": rng.choice(["0건", "1건", "2건", "3건 이상"]),
        "SCRB_PATH_NM_GRP": rng.choice(["현장경로", "O/B", "I/B", "일반상담", "직영몰", "기타", "정보없음"]),
        "INHOME_RATE": rng.choice(["0.0", "10.0", "20.0", "30.0", "40.0", "50.0", "알수없음"]),
        "AGMT_END_SEG": rng.choice(["약정만료전 12개월이상", "약정만료전 6~9개월", "약정만료후 0~3개월", "약정만료후 12개월이상", "기타"]),
        "AGMT_END_YMD": rng.choice(["20250213", "20260214", "무약정", "정보없음"]),
        "TOTAL_USED_DAYS": str(rng.randint(30, 3000)),
        "TV_SCRB": str(tv), "ANALOG_SCRB": str(rng.randint(0, 1)), "DIGITAL_SCRB": str(tv),
        "TOTAL_INTERNET_SCRB": str(rng.randint(0, 2)), "GIGA_INTERNET_SCRB": str(rng.randint(0, 1)),
        "BUNDLE_YN": rng.choice(YN), "DIGITAL_GIGA_YN": rng.choice(YN), "DIGITAL_ALOG_YN": rng.choice(YN),
        "TV_I_CNT": f"{float(tv)}",
        "AGE_GRP10": rng.choice(["20대", "30대", "40대", "50대", "60대", "70대", "80대", "연령없음"]),
        "EMAIL_RECV_CLS_NM": rng.choice(["수신", "광고거부", "전체거부"]),
        "SMS_SEND_CLS_NM": rng.choice(["수신", "광고거부", "전체거부"]),
        "CH_FAV_RNK1": rng.choice(["기타", "기타", "KBS1", "KBS2", "MBC", "SBS", "연합뉴스TV"]),
    }


def monthly_values(rng: random.Random) -> dict:
    ratio = round(rng.uniform(0, 30), 2)
    return {
        "CH_LAST_DAYS_BF_GRP": rng.choice(["일주일내", "2주일내", "1개월내", "3개월내없음"]),
        "VOC_TOTAL_MONTH1_YN": rng.choice(YN), "VOC_STOP_CANCEL_MONTH1_YN": rng.choice(["N", "N", "N", "Y"]),
        "CH_HH_AVG_MONTH1": f"{rng.uniform(0, 9.99):.2f}",
        "CH_25_RATIO_MONTH1": f"{ratio}",
        "CH_25_RATIO_MEAN_3MM": f"{ratio}",
        "KIDS_USE_PV_MONTH1": f"{float(rng.choice([0, 0, 0, 0, 1, 3]))}",
        "NFX_USE_YN": rng.choice(YN), "YTB_USE_YN": rng.choice(YN),
    }


def generate(n_customers: int, seed: int, inject_bad: bool) -> dict[str, list[dict]]:
    rng = random.Random(seed)
    rows_by_month = {m: [] for m in MONTHS}

    for i in range(n_customers):
        prof = customer_profile(rng, i)
        start = rng.randint(0, 3)
        churn_at = rng.randint(start, len(MONTHS) + 20)  # 범위를 넘으면 해지 안 함
        end = min(len(MONTHS), churn_at + rng.randint(1, 2)) if churn_at < len(MONTHS) else len(MONTHS)
        conflicted_from = rng.randint(start, len(MONTHS) - 1) if i % 100 == 7 else None

        for k in range(start, end):
            row = {**prof, **monthly_values(rng), "p_mt": MONTHS[k],
                   "cancel_yn": "해지" if k >= churn_at else "유지"}
            rows_by_month[MONTHS[k]].append(row)
            # 충돌 고객: 특정 달부터 이후 모든 달에 cancel_yn만 다른 행이 하나 더 있음
            if conflicted_from is not None and k >= conflicted_from:
                twin = dict(row, cancel_yn="유지" if row["cancel_yn"] == "해지" else "해지")
                rows_by_month[MONTHS[k]].append(twin)

    all_rows = [r for m in MONTHS for r in rows_by_month[m]]

    # 오염 행: 여러 컬럼이 동시에 센티널
    for r in rng.sample(all_rows, 5):
        r.update({"STB_RES_1M_YN": '""', "TV_SCRB": '""', "ANALOG_SCRB": '""',
                  "SVOD_SCRB_CNT_GRP": "기타", "AGMT_END_SEG": "기타"})
    # 셋톱박스 휴면 값이 비어 있는(NULL) 행: 오염 행이 아니므로 제외되면 안 됨
    for r in rng.sample(all_rows, 2):
        r["STB_RES_1M_YN"] = ""
    # 10대미만
    for r in rng.sample(all_rows, 2):
        r["AGE_GRP10"] = "10대미만"
    # 비율 100 초과 (신규 고객 edge case)
    for r in rng.sample(all_rows, 3):
        r["CH_25_RATIO_MONTH1"] = "124.5"
    # 품질검사 실패 시연: 숫자 컬럼에 문자열이 섞인 새 데이터가 들어온 상황
    if inject_bad:
        for r in rng.sample(all_rows, 4):
            r["TOTAL_USED_DAYS"] = "N/A"

    return {fname: [r for m in months for r in rows_by_month[m]] for fname, months in QUARTER_FILES.items()}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--customers", type=int, default=3000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--inject-bad", action="store_true", help="품질검사 실패 시연용 불량 데이터 포함")
    parser.add_argument("--spec-names", action="store_true",
                        help="샘플 파일의 컬럼 이름으로 저장 (스키마 검사 실패 시연용)")
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    # 실제 데이터를 실수로 지우지 않도록, 샘플이 아닌 CSV가 있으면 아무것도 하지 않고 멈춘다
    others = [p.name for p in OUT_DIR.glob("*.csv") if p.name not in QUARTER_FILES]
    if others:
        print(f"중단: {OUT_DIR} 에 샘플이 아닌 CSV가 있습니다 → {others}")
        print("실제 데이터가 지워지지 않도록 샘플을 만들지 않았습니다. 다른 폴더로 옮긴 뒤 다시 실행하세요.")
        sys.exit(1)
    for old in OUT_DIR.glob("*.csv"):
        old.unlink()
    files = generate(args.customers, args.seed, args.inject_bad)
    for fname, rows in files.items():
        with open(OUT_DIR / fname, "w", newline="", encoding="utf-8") as fp:
            names = [SPEC_NAMES.get(c, c) for c in COLUMNS] if args.spec_names else COLUMNS
            w = csv.writer(fp)
            w.writerow(names)
            w.writerows([[r[c] for c in COLUMNS] for r in rows])
        print(f"{fname}: {len(rows):,}행")


if __name__ == "__main__":
    main()
