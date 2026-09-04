-- Tags: no-parallel-replicas
-- `make_distributed_plan` forces `use_skip_indexes_on_data_read = 0`
-- (`adjustSettingsForMakeDistributedPlan`), which turns the direct read off whatever
-- `query_plan_direct_read_from_text_index` is set to below, so those runs would exercise only the
-- fallback path and never see this regression.

-- A direct read from a text index answers the search predicate from a column the index reader
-- synthesizes, in a read step of its own: that reader produces exactly one column and reads nothing
-- from the part. A pending lightweight update adds a patch part, and the reader chain applies it by
-- reading the patch key columns in the first read step. Those two collided - the key columns were
-- added to the index step, which then had to be served by the main reader, which cannot produce the
-- index column, so the filter saw only its default and the query silently returned no rows at all.
--
-- The discriminator is whether the query materializes a column the patch updates: `SELECT id` or a
-- bare `count()` were unaffected, and `sum(c)` looked like a stale value only because it was a sum
-- over the empty set.

-- The runner randomizes this setting off in a fraction of the runs, which would turn every arm below
-- into a copy of the `query_plan_direct_read_from_text_index = 0` arm and hide the regression.
SET query_plan_direct_read_from_text_index = 1;
SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS t_text_index_patch;
CREATE TABLE t_text_index_patch (id UInt64, c UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY id
-- `SYSTEM STOP MERGES` below only stops merges on this replica, so on a shared or replicated table
-- another one could materialize the patch and quietly turn every arm into the unpatched case.
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0;

INSERT INTO t_text_index_patch SELECT number, 0, concat('tok', toString(number % 10), ' word') FROM numbers(1000);

SYSTEM STOP MERGES t_text_index_patch;
UPDATE t_text_index_patch SET c = 1 WHERE id < 10;

-- 100 rows match `tok1`, of which only `id = 1` was updated.
SELECT count(), sum(c) FROM t_text_index_patch WHERE hasToken(s, 'tok1');
SELECT count(), sum(c) FROM t_text_index_patch WHERE hasToken(s, 'tok1') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT count(), sum(c) FROM t_text_index_patch WHERE hasToken(s, 'tok1') SETTINGS use_skip_indexes = 0;

-- A second predicate on top of the index read: the rows disappeared here too.
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
-- it is used again - otherwise a guard that never releases would keep this arm green.
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT count() FROM t_text_index_patch WHERE hasToken(s, 'tok1')
) WHERE explain LIKE '%\_\_text_index%';

DROP TABLE t_text_index_patch;

-- A patch of the indexed column is a different case, already excluded by `canUseIndex`, because
-- `AlterConversions::addPatchPart` records patched columns in `getAllUpdatedColumns` as well. Kept
-- here so that a future narrowing of the new guard cannot silently start answering from the stale
-- index: `zebra` is not in the index at all, and `id = 1` must stop matching `tok1`.
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
