-- `rewrite_in_to_join` is a production-tier setting that is implemented only in the analyzer.
-- With the old analyzer it is a documented no-op: the query is accepted, `IN` is evaluated as usual
-- and no `JOIN` appears in the plan.

SET explain_query_plan_default = 'legacy';
SET rewrite_in_to_join = 1;

-- {echoOn}
SET enable_analyzer = 0;

SELECT number IN (SELECT number FROM numbers(2)) FROM numbers(3) ORDER BY number;
SELECT * FROM numbers(3) WHERE number IN (SELECT number FROM numbers(2)) ORDER BY number;
SELECT * FROM numbers(3) WHERE number NOT IN (SELECT number FROM numbers(2)) ORDER BY number;

SELECT count() FROM (
    EXPLAIN SELECT * FROM numbers(3) WHERE number IN (SELECT number FROM numbers(2))
) WHERE explain ILIKE '%join%';

-- The same queries with the analyzer produce identical results.
SET enable_analyzer = 1;

SELECT number IN (SELECT number FROM numbers(2)) FROM numbers(3) ORDER BY number;
SELECT * FROM numbers(3) WHERE number IN (SELECT number FROM numbers(2)) ORDER BY number;
SELECT * FROM numbers(3) WHERE number NOT IN (SELECT number FROM numbers(2)) ORDER BY number;

-- ... but with the analyzer the rewrite does happen, so the plan contains a `JOIN`.
SELECT count() > 0 FROM (
    EXPLAIN SELECT * FROM numbers(3) WHERE number IN (SELECT number FROM numbers(2))
) WHERE explain ILIKE '%join%';
