-- 01b_dup_rows.sql
-- [1단계-2] 원본 파일 1개에서, 01a 에서 찾은 중복 (고객, 달)에 해당하는 줄만 골라
--          줄 전체 내용을 해시(row_hash)로 만들어 dup_rows 테이블에 추가한다.
--
-- 파일을 하나씩 처리하는 이유
--   파일 4개의 모든 컬럼을 한꺼번에 읽었더니 실제 데이터(2,289만 줄)에서 메모리가 부족했다.
--   (임시 디스크 한도 2GiB 초과) 파일 1개씩 하면 문제없이 된다. 2단계 정제와 같은 방식.
--   해시는 전부 dup_rows 한 테이블에 모은 다음 01c 에서 한꺼번에 비교하므로,
--   같은 고객의 줄이 다른 파일에 나뉘어 있어도 결과는 똑같다.
--
-- row_hash 만드는 방법: md5(CAST(t AS VARCHAR))
--   - t 는 read_csv 결과에 붙인 별칭. t 만 쓰면 한 줄 전체가 하나의 값(STRUCT)이 된다.
--   - 그걸 문자열로 바꾸고 md5 로 해시하면, 모든 컬럼이 같은 줄끼리는 해시도 같다.
--   - 긴 문자열 대신 32글자 해시를 저장해서 메모리를 아낀다.

INSERT INTO dup_rows
SELECT
    t.sha2_hash,
    t.p_mt,
    t.cancel_yn,
    md5(CAST(t AS VARCHAR)) AS row_hash
FROM read_csv({raw_file}, all_varchar = true, union_by_name = true) AS t
JOIN dup_keys d
  ON t.sha2_hash = d.sha2_hash
 AND t.p_mt = d.p_mt;

-- 조기필터링
-- 빨리 거르기 (지금):  CSV → [고객+달로 JOIN: 필요한 줄만] → 저장 → 비교
-- 늦게 거르기:        CSV → [고객으로 JOIN: 복사된 줄까지] → 저장 → [비교하면서 버림]
