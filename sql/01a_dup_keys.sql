-- 01a_dup_keys.sql
-- [1단계-1] 같은 고객(sha2_hash)이 같은 달(p_mt)에 2줄 이상 있는 (고객, 달) 목록을 찾는다.
--
-- 1단계 전체 흐름 (run.py 의 find_conflicted_customers)
--   01a_dup_keys.sql            : 중복된 (고객, 달) 찾기           <- 이 파일 (파일 4개를 한꺼번에)
--   01b_dup_rows.sql            : 중복 줄의 내용을 해시로 저장      (파일 1개씩, 4번 반복)
--   01c_conflicted_customers.sql: 중복을 3종류로 나누고 뺄 고객 결정
--
-- 한 고객의 데이터가 여러 파일(분기)에 걸쳐 있으므로 이 단계는 파일 4개를 한꺼번에 읽는다.
-- 대신 컬럼 2개만 읽어서 가볍게 처리한다.

CREATE OR REPLACE TEMP TABLE dup_keys AS
SELECT sha2_hash, p_mt
FROM read_csv({raw_files}, all_varchar = true, union_by_name = true)
GROUP BY sha2_hash, p_mt
HAVING count(*) > 1;

-- 01b_dup_rows.sql 이 파일마다 여기에 줄을 추가한다
CREATE OR REPLACE TEMP TABLE dup_rows (
    sha2_hash VARCHAR,
    p_mt      VARCHAR,
    cancel_yn VARCHAR,
    row_hash  VARCHAR
);
