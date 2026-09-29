"""테스트에서 같이 쓰는 도우미들.

테스트는 실제 데이터가 아니라 scripts/make_sample_data.py 로 만든 가짜 샘플 데이터로 돌린다.
결과 폴더(clean, mart, logs)는 pytest 가 주는 임시 폴더(tmp_path)로 바꿔서,
실제 data/ 폴더는 절대 건드리지 않는다.
"""
import csv
import json
import logging
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

import make_sample_data  # noqa: E402
from pipeline import config  # noqa: E402
from pipeline import run as pipeline_run  # noqa: E402


def write_csv_files(folder, files, use_spec_names=False):
    """make_sample_data.generate() 가 만든 데이터를 CSV 파일로 저장한다.

    use_spec_names=True 이면 컬럼 이름을 설명 문서 버전(틀린 이름)으로 저장한다. (스키마 검사 테스트용)
    """
    folder.mkdir(parents=True, exist_ok=True)
    for file_name, rows in files.items():
        if use_spec_names:
            header = []
            for col in make_sample_data.COLUMNS:
                header.append(make_sample_data.SPEC_NAMES.get(col, col))
        else:
            header = make_sample_data.COLUMNS

        with open(folder / file_name, "w", newline="", encoding="utf-8") as fp:
            writer = csv.writer(fp)
            writer.writerow(header)
            for row in rows:
                writer.writerow([row[col] for col in make_sample_data.COLUMNS])


def read_report(log_dir):
    """가장 최근 품질 리포트(JSON)를 읽어서 딕셔너리로 돌려준다."""
    reports = sorted(log_dir.glob("quality_report_*.json"))
    return json.loads(reports[-1].read_text(encoding="utf-8"))


def find_check(report, check_name):
    """리포트에서 이름이 check_name 인 검사 결과를 찾는다."""
    all_checks = report.get("clean_checks", []) + report.get("mart_checks", [])
    for check in all_checks:
        if check["name"] == check_name:
            return check
    return None


@pytest.fixture
def pipeline_env(tmp_path, monkeypatch):
    """결과 폴더를 임시 폴더로 바꾼 환경을 만들어 준다.

    테스트에서는 env["run"](files) 처럼 불러서 파이프라인을 돌린다.
    돌려주는 값: (종료 코드, 품질 리포트 딕셔너리)
    """
    monkeypatch.setattr(config, "CLEAN_DIR", tmp_path / "clean" / "churn_clean")
    monkeypatch.setattr(config, "MART_DIR", tmp_path / "mart")
    monkeypatch.setattr(config, "LOG_DIR", tmp_path / "logs")

    run_count = [0]   # 같은 테스트에서 여러 번 돌릴 때 원본 폴더 이름이 겹치지 않게

    def run(files, use_spec_names=False):
        run_count[0] += 1
        raw_dir = tmp_path / f"raw_{run_count[0]}"
        write_csv_files(raw_dir, files, use_spec_names)
        exit_code = pipeline_run.run(str(raw_dir / "*.csv"))
        return exit_code, read_report(config.LOG_DIR)

    env = {
        "run": run,
        "clean_dir": tmp_path / "clean" / "churn_clean",
        "mart_dir": tmp_path / "mart",
    }
    yield env

    # 로그 파일을 닫아준다 (Windows 에서는 열린 파일을 지울 수 없어서)
    logger = logging.getLogger("churn_pipeline")
    for handler in list(logger.handlers):
        handler.close()
        logger.removeHandler(handler)


@pytest.fixture
def sample_files():
    """정상 샘플 데이터 (고객 1,000명). 테스트마다 새로 만들어서 서로 영향을 주지 않는다."""
    return make_sample_data.generate(n_customers=1000, seed=42, inject_bad=False)
