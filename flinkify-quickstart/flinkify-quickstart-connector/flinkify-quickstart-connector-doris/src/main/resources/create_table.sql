CREATE TABLE test
(
    siteid   INT         DEFAULT '10',
    city     VARCHAR(32) DEFAULT '',
    username VARCHAR(32) DEFAULT '',
    pv       BIGINT SUM DEFAULT '0'
) AGGREGATE KEY(siteid, city, username)
DISTRIBUTED BY HASH(siteid) BUCKETS 10
PROPERTIES("replication_num" = "1");

INSERT INTO test
VALUES (1, 'New York', 'John', 100),
       (1, 'Los Angeles', 'Mike', 120),
       (2, 'Chicago', 'Bill', 200);
