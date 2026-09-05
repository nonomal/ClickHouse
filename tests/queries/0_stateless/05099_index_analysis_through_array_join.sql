-- Tags: no-parallel-replicas
-- A condition on an ARRAY JOIN element reaches the skip index as `arrayJoin(col)`, like `has(col, x)` does.

DROP TABLE IF EXISTS t_aj_index;

CREATE TABLE t_aj_index (id UInt64, tags Array(String), INDEX idx_tags tags TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

-- Each tag is unique, so a tag lives in exactly one granule: 100000 rows / 8192 = 13 granules.
INSERT INTO t_aj_index SELECT number, [concat('tag_', toString(number))] FROM numbers(100000);

-- Baseline: hasAny keeps the matching granule plus one bloom filter false positive.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_aj_index WHERE hasAny(tags, ['tag_42'])) WHERE explain ILIKE '%Granules: 2/13%';

-- The clause form prunes the same way, and the result does not depend on the index.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_aj_index ARRAY JOIN tags AS t WHERE t IN ('tag_42')) WHERE explain ILIKE '%Granules: 2/13%';
SELECT count() FROM t_aj_index ARRAY JOIN tags AS t WHERE t IN ('tag_42') SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_aj_index ARRAY JOIN tags AS t WHERE t IN ('tag_42') SETTINGS use_skip_indexes = 0;

-- The function form keeps its pruning when it is lowered to an ARRAY JOIN step.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_aj_index WHERE arrayJoin(tags) = 'tag_42' SETTINGS query_plan_lower_array_join_function = 1) WHERE explain ILIKE '%Granules: 2/13%';
SELECT count() FROM t_aj_index WHERE arrayJoin(tags) = 'tag_42' SETTINGS query_plan_lower_array_join_function = 1;

-- LEFT ARRAY JOIN turns an empty array into a default-valued row, so the index must stay out of the plan.
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_aj_index LEFT ARRAY JOIN tags AS t WHERE t IN ('tag_42')) WHERE explain ILIKE '%Name: idx_tags%';

DROP TABLE t_aj_index;

-- A LIMIT above the join is not pushed into the source: the first 90 arrays are empty, so a source capped at 3 rows would yield nothing.
SELECT count() FROM (SELECT arrayJoin(if(number < 90, [], [number])) FROM numbers(100) LIMIT 3) SETTINGS query_plan_lower_array_join_function = 1;
