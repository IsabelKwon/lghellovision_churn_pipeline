-- 02_drop_rules.sql
-- 원본 CSV 파일 1개를 읽어서, 모든 줄에 "제외 사유(_drop_reason)" 컬럼을 붙인다.
--   - 제외할 줄   : _drop_reason 에 사유가 들어감 (예: 'R1_corrupted_row')
--   - 남길 줄     : _drop_reason 이 NULL
--
-- 이 뷰(raw_tagged)를 두 곳에서 같이 쓴다.
--   1) run.py      : 사유별로 몇 줄이 빠졌는지 센다
--   2) 03_clean.sql: _drop_reason 이 NULL 인 줄만 가져가서 정제한다
-- 같은 뷰를 쓰기 때문에 "센 개수"와 "실제로 뺀 개수"가 항상 같다.
--
-- WHERE 대신 CASE 를 쓰는 이유
--   예전에는 WHERE STB_RES_1M_YN != '""' 로 걸렀는데, 값이 NULL 인 줄은
--   NULL != '""' 결과가 NULL 이 되어서 정상 줄인데도 같이 빠져버렸다.
--   CASE 는 조건이 NULL 이면 그냥 다음 WHEN 으로 넘어가서 이런 문제가 없다.
--
-- 한 줄이 여러 규칙에 걸리면 위에 있는 WHEN 하나만 적용된다. (두 번 세지 않기 위해)

CREATE OR REPLACE VIEW raw_tagged AS
SELECT
    *,
    CASE
        -- [R1] 여러 컬럼에 동시에 이상한 값이 들어간 오염된 줄 (테스트 계정 등으로 추정)
        WHEN STB_RES_1M_YN = '""' THEN 'R1_corrupted_row'

        -- [R2] '10대미만': 한 분기에만 몇 건 나오고 표본이 너무 적음
        WHEN AGE_GRP10 = '10대미만' THEN 'R2_age_under_10'

        -- [R3a] 같은 달에 유지/해지가 동시에 있는 고객 -> 고객의 모든 달을 뺀다
        WHEN sha2_hash IN (SELECT sha2_hash FROM conflicted_customers
                           WHERE dup_type = 'label_conflict') THEN 'R3a_label_conflict'

        -- [R3c] 같은 달에 라벨은 같지만 다른 값이 다른 줄이 있는 고객 -> 고객의 모든 달을 뺀다
        WHEN sha2_hash IN (SELECT sha2_hash FROM conflicted_customers
                           WHERE dup_type = 'value_mismatch') THEN 'R3c_value_mismatch'

        -- [R3b] 완전히 똑같은 줄이 여러 개 -> 첫 번째 줄만 남기고 나머지를 뺀다
        --   R3a, R3c 고객은 위에서 이미 걸렀기 때문에,
        --   여기까지 왔는데 같은 (고객, 달)의 2번째 이후 줄이라면 그건 완전 중복뿐이다.
        --   줄이 전부 똑같아서 어느 줄을 남겨도 결과는 같다.
        WHEN row_num > 1 THEN 'R3b_exact_duplicate'
    END AS _drop_reason
FROM (
    -- 같은 (고객, 달) 안에서 몇 번째 줄인지 번호를 붙인다 (1, 2, 3, ...)
    --
    -- 이 자리에는 run.py 의 clean_raw_files() 가 둘 중 하나를 넣는다.
    --   완전 중복이 있으면 : row_number() OVER (PARTITION BY sha2_hash, p_mt)
    --   완전 중복이 없으면 : 1   (모든 줄의 번호를 1로 고정)
    -- row_number() 는 파일의 모든 줄을 (고객, 달)별로 모아야 해서 무겁다.
    -- 실제 데이터로 재보니 2단계가 약 2배 느려졌는데, 실제 데이터에는 완전 중복이 0건이다.
    -- 완전 중복이 없으면 번호가 2 이상인 줄은 전부 R3a/R3c 고객이라 위에서 먼저 걸러지므로,
    -- 번호를 1로 고정해도 결과는 똑같다.
    SELECT
        *,
        {row_num_sql} AS row_num
    FROM read_csv({raw_file}, all_varchar = true, union_by_name = true)
);
