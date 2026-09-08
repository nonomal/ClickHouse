-- Tags: no-parallel-replicas
-- no-parallel-replicas: the distributed plan forces `use_skip_indexes_on_data_read = 0`, which turns
-- the direct read off, so those runs would only ever exercise the fallback path.

-- Checks that a text index search returns the same rows while a lightweight update is pending on the
-- table as it does without one, and as it does with the direct read from the text index turned off.
-- Only queries that read a column the update touches were ever affected, so that is what the arms
-- below vary.

-- Pinned: the runner randomizes it off, which would collapse every arm into the fallback path.
SET query_plan_direct_read_from_text_index = 1;
SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS t_text_index_patch;
CREATE TABLE t_text_index_patch (id UInt64, c UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY id
-- `apply_patches_on_merge = 0` keeps the patch pending: `SYSTEM STOP MERGES` alone is per-replica.
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0;

INSERT INTO t_text_index_patch SELECT number, 0, concat('tok', toString(number % 10), ' word') FROM numbers(1000);

SYSTEM STOP MERGES t_text_index_patch;
UPDATE t_text_index_patch SET c = 1 WHERE id < 10;

-- 100 rows match `tok1`, of which only `id = 1` was updated.
SELECT count(), sum(c) FROM t_text_index_patch WHERE hasToken(s, 'tok1');
SELECT count(), sum(c) FROM t_text_index_patch WHERE hasToken(s, 'tok1') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT count(), sum(c) FROM t_text_index_patch WHERE hasToken(s, 'tok1') SETTINGS use_skip_indexes = 0;

-- A second predicate alongside the search: the rows disappeared here too.
SELECT id, c FROM t_text_index_patch WHERE hasToken(s, 'tok1') AND id < 25 ORDER BY id;

-- Reading only columns the patch does not update was never affected; keep it as the control arm.
SELECT count() FROM t_text_index_patch WHERE hasToken(s, 'tok1');
SELECT count() FROM (SELECT id FROM t_text_index_patch WHERE hasToken(s, 'tok1'));

SELECT count(), sum(c) FROM t_text_index_patch WHERE hasAnyTokens(s, ['tok1', 'tok2']);
SELECT count(), sum(c) FROM t_text_index_patch WHERE hasAnyTokens(s, ['tok1', 'tok2']) SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT count(), sum(c) FROM t_text_index_patch WHERE hasAllTokens(s, ['tok1', 'word']);
SELECT count(), sum(c) FROM t_text_index_patch WHERE hasAllTokens(s, ['tok1', 'word']) SETTINGS query_plan_direct_read_from_text_index = 0;

-- And the same once the patch has materialized, where the direct read is used again.
SELECT 'materialized';
SYSTEM START MERGES t_text_index_patch;
ALTER TABLE t_text_index_patch MODIFY SETTING apply_patches_on_merge = 1;
OPTIMIZE TABLE t_text_index_patch FINAL;
SELECT count(), sum(c) FROM t_text_index_patch WHERE hasToken(s, 'tok1');
-- The results above are the same whether or not the direct read is used, so assert separately that
-- it is used again - otherwise this arm would stay green even if it never came back.
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT count() FROM t_text_index_patch WHERE hasToken(s, 'tok1')
) WHERE explain LIKE '%\_\_text_index%';

DROP TABLE t_text_index_patch;

-- A text index declared with a preprocessor: the search has to keep applying it while an update is
-- pending on some other column. Giving up the whole index analysis instead of only the read from the
-- index would drop `lower(s)` and make `Hello World` stop matching `hello`.
DROP TABLE IF EXISTS t_text_index_preprocessor;

CREATE TABLE t_text_index_preprocessor
(
    id UInt64,
    c UInt64,
    s String,
    INDEX idx_s s TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(s))
)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0;

INSERT INTO t_text_index_preprocessor SELECT number, 0, if(number < 10, 'Hello World', 'foo bar') FROM numbers(1000);

-- The preprocessor applies on the index path only, so these two legitimately differ.
SELECT count() FROM t_text_index_preprocessor WHERE hasAnyTokens(s, 'hello') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_text_index_preprocessor WHERE hasAnyTokens(s, 'hello') SETTINGS use_skip_indexes = 1;

SYSTEM STOP MERGES t_text_index_preprocessor;
UPDATE t_text_index_preprocessor SET c = 1 WHERE id = 500;

-- Same two answers with an update pending on `c`, which the index does not cover.
SELECT count() FROM t_text_index_preprocessor WHERE hasAnyTokens(s, 'hello') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_text_index_preprocessor WHERE hasAnyTokens(s, 'hello') SETTINGS use_skip_indexes = 1;

DROP TABLE t_text_index_preprocessor;

-- Updating the indexed column itself: the search has to see the new value rather than answer `zebra`
-- or `tok1` from the index built before the update.
SELECT 'patched indexed column';

DROP TABLE IF EXISTS t_text_index_patch_indexed;
CREATE TABLE t_text_index_patch_indexed (id UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0;

INSERT INTO t_text_index_patch_indexed SELECT number, concat('tok', toString(number % 10), ' word') FROM numbers(1000);

SYSTEM STOP MERGES t_text_index_patch_indexed;
UPDATE t_text_index_patch_indexed SET s = 'zebra word' WHERE id = 1;

SELECT count() FROM t_text_index_patch_indexed WHERE hasToken(s, 'tok1');
SELECT count() FROM t_text_index_patch_indexed WHERE hasToken(s, 'tok1') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT count() FROM t_text_index_patch_indexed WHERE hasToken(s, 'zebra');
SELECT count() FROM t_text_index_patch_indexed WHERE hasToken(s, 'zebra') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT count() FROM t_text_index_patch_indexed WHERE hasToken(s, 'zebra') SETTINGS use_skip_indexes = 0;

DROP TABLE t_text_index_patch_indexed;
