-- Tags: distributed

SET send_logs_level = 'fatal';
SET prefer_localhost_replica = 1;
SET parallel_distributed_insert_select = 2;

DROP TABLE IF EXISTS local_03826_src;
DROP TABLE IF EXISTS local_03826_dst;
DROP TABLE IF EXISTS distributed_03826_src;
DROP TABLE IF EXISTS distributed_03826_dst;

CREATE TABLE local_03826_src (col String) ENGINE = MergeTree ORDER BY col;
CREATE TABLE local_03826_dst (col String) ENGINE = MergeTree ORDER BY col;
CREATE TABLE distributed_03826_src AS local_03826_src ENGINE = Distributed('test_shard_localhost', currentDatabase(), local_03826_src);
CREATE TABLE distributed_03826_dst AS local_03826_dst ENGINE = Distributed('test_shard_localhost', currentDatabase(), local_03826_dst);

INSERT INTO local_03826_src VALUES ('value1'), ('value2');

INSERT INTO distributed_03826_dst
WITH cte AS (SELECT * FROM distributed_03826_src) SELECT cte.col FROM cte AS c;

SELECT * FROM local_03826_dst ORDER BY col;
TRUNCATE TABLE local_03826_dst;

INSERT INTO distributed_03826_dst
WITH cte AS (SELECT * FROM distributed_03826_src) SELECT c.col FROM cte AS c;

SELECT * FROM local_03826_dst ORDER BY col;
TRUNCATE TABLE local_03826_dst;

INSERT INTO distributed_03826_dst
WITH cte AS (SELECT * FROM distributed_03826_src) SELECT cte.col FROM cte;

SELECT * FROM local_03826_dst ORDER BY col;

DROP TABLE local_03826_src;
DROP TABLE local_03826_dst;
DROP TABLE distributed_03826_src;
DROP TABLE distributed_03826_dst;
