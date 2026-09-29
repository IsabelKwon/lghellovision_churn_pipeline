-- 03_clean.sql
-- 02_drop_rules.sql 에서 제외되지 않은 줄(_drop_reason 이 NULL)만 가져와서 값을 정리한다.
-- 값을 바꾸는 규칙(R4~R9)은 전부 이 파일 하나에 있다.
--
-- 예전에는 노트북에서 Parquet 파일을 3번 덮어쓰면서 조금씩 고쳤는데,
-- 그러면 최종 파일이 어떤 순서로 만들어졌는지 알기 어려웠다.
-- 그래서 모든 규칙을 SELECT 하나에 모았다. (원본 CSV + 이 파일 = 항상 같은 결과)
--
-- 문법 설명 (DuckDB)
--   SELECT * EXCLUDE (a, b)   : 모든 컬럼을 가져오되 a, b 는 빼기
--   SELECT * REPLACE (식 AS c): 모든 컬럼을 가져오되 c 컬럼만 식의 결과로 바꾸기
--   -> 원래 컬럼 순서가 그대로 유지되고, 새 컬럼(AGMT_END_DATE, IS_NO_AGMT)만 맨 뒤에 붙는다.
--
-- TRY_CAST 는 변환에 실패하면 에러 대신 NULL 을 돌려준다.
-- 그래서 예상 못 한 NULL 이 생기지 않았는지 quality.py 의 check_unexpected_nulls 에서 따로 확인한다.

SELECT * EXCLUDE (_drop_reason, row_num) REPLACE (
    -- [R4] '정보없음'은 사실상 빈 값이므로 NULL 로 바꾼다. ('무약정'은 진짜 값이라 그대로 둠)
    NULLIF(AGMT_KIND_NM, '정보없음') AS AGMT_KIND_NM,
    NULLIF(AGMT_END_YMD, '정보없음') AS AGMT_END_YMD,

    -- [R5] 정제 후 다시 검사하다가 여기서도 '정보없음'을 발견해서 추가함
    NULLIF(SCRB_PATH_NM_GRP, '정보없음') AS SCRB_PATH_NM_GRP,

    -- [R6] '알수없음' -> NULL, 나머지는 숫자(0~100)로 변환
    TRY_CAST(NULLIF(INHOME_RATE, '알수없음') AS DOUBLE) AS INHOME_RATE,

    -- [R7] 비율인데 100 을 넘는 값이 10건 있음 (신규 고객 평균 계산 오류로 추정) -> 100 으로 자름
    --      LEAST(값, 100) 을 쓰지 않은 이유: DuckDB 의 LEAST 는 NULL 을 무시해서 LEAST(NULL, 100) = 100 이 된다.
    --      그러면 빈 값이나 숫자가 아닌 값('N/A' 등)이 조용히 100 으로 바뀌고, check_unexpected_nulls 도 못 잡는다.
    --      CASE 는 NULL > 100 이 참이 아니라서 ELSE 로 가고, NULL 이 그대로 남는다.
    CASE
        WHEN TRY_CAST(CH_25_RATIO_MONTH1 AS DOUBLE) > 100 THEN 100
        ELSE TRY_CAST(CH_25_RATIO_MONTH1 AS DOUBLE)
    END AS CH_25_RATIO_MONTH1,

    -- [R8] CSV 를 전부 문자열로 읽었으니 숫자 컬럼은 DOUBLE 로 바꾼다 (모델 학습에 필요)
    TRY_CAST(TOTAL_USED_DAYS      AS DOUBLE) AS TOTAL_USED_DAYS,
    TRY_CAST(TV_SCRB              AS DOUBLE) AS TV_SCRB,
    TRY_CAST(ANALOG_SCRB          AS DOUBLE) AS ANALOG_SCRB,
    TRY_CAST(DIGITAL_SCRB         AS DOUBLE) AS DIGITAL_SCRB,
    TRY_CAST(TOTAL_INTERNET_SCRB  AS DOUBLE) AS TOTAL_INTERNET_SCRB,
    TRY_CAST(GIGA_INTERNET_SCRB   AS DOUBLE) AS GIGA_INTERNET_SCRB,
    TRY_CAST(TV_I_CNT             AS DOUBLE) AS TV_I_CNT,
    TRY_CAST(CH_HH_AVG_MONTH1     AS DOUBLE) AS CH_HH_AVG_MONTH1,
    TRY_CAST(CH_25_RATIO_MEAN_3MM AS DOUBLE) AS CH_25_RATIO_MEAN_3MM,
    TRY_CAST(KIDS_USE_PV_MONTH1   AS DOUBLE) AS KIDS_USE_PV_MONTH1
),
    -- [R9] AGMT_END_YMD 에 날짜('20251201')와 글자('무약정')가 섞여 있다.
    --      이대로 문자열 비교를 하면 '무약정' > '20240101' 이 참이 되어 결과가 틀린다.
    --      그래서 날짜 컬럼과 무약정 여부 컬럼을 새로 만든다. (원본 AGMT_END_YMD 는 확인용으로 남김)
    CAST(TRY_STRPTIME(AGMT_END_YMD, '%Y%m%d') AS DATE) AS AGMT_END_DATE,
    CASE
        WHEN AGMT_END_YMD IS NULL THEN NULL
        WHEN AGMT_END_YMD = '정보없음' THEN NULL
        WHEN AGMT_END_YMD = '무약정' THEN TRUE
        ELSE FALSE
    END AS IS_NO_AGMT
FROM raw_tagged
WHERE _drop_reason IS NULL
