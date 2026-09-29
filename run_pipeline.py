"""파이프라인 실행 진입점.

    .venv\\Scripts\\activate                         # 먼저 가상환경 켜기 (Windows)
    python run_pipeline.py                         # data/raw/*.csv 사용
    python run_pipeline.py --raw "경로/*.csv"       # 원본 위치 지정
"""
import argparse
import sys

from pipeline import config
from pipeline.run import run

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LG헬로비전 해지 데이터 정제 파이프라인")
    parser.add_argument("--raw", default=config.RAW_GLOB, help="원본 CSV glob 패턴")
    args = parser.parse_args()
    sys.exit(run(args.raw))
    
# ArgumentParser()   빈 해석기 만들기
# add_argument()     받을 옵션 알려주기 (빈칸 만들기)
# parse_args()       실제 명령어를 읽고 해석하기 (빈칸 채우기)