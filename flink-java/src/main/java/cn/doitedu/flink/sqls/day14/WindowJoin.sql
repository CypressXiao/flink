-- 让两个流实现window的join
-- 窗口一个第一个流对应的表
CREATE TABLE LeftTable (
  `row_time` TIMESTAMP(3),
  `num` INT,
  `id` STRING,
  WATERMARK FOR `row_time` AS row_time - INTERVAL '1' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'tp-left1',
  'properties.bootstrap.servers' = 'hadoop001:9092,hadoop002:9092,hadoop003:9092',
  'properties.group.id' = 'testGroup1',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'csv',
  'csv.ignore-parse-errors' = 'true'
  )
;

CREATE TABLE RightTable (
  `row_time` TIMESTAMP(3),
  `num` INT,
  `id` STRING,
  WATERMARK FOR `row_time` AS row_time - INTERVAL '1' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'tp-right1',
  'properties.bootstrap.servers' = 'hadoop001:9092,hadoop002:9092,hadoop003:9092',
  'properties.group.id' = 'testGroup2',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'csv',
  'csv.ignore-parse-errors' = 'true'
  )
;

select
  L.num as L_Num, L.id as L_Id, R.num as R_Num, R.id as R_Id, L.window_start, L.window_end
from
(
  SELECT
    *
  FROM TABLE(
    TUMBLE(TABLE LeftTable, DESCRIPTOR(row_time), INTERVAL '5' MINUTES)
  )
) L
FULL JOIN
(
  SELECT
    *
  FROM TABLE(
    TUMBLE(TABLE RightTable, DESCRIPTOR(row_time), INTERVAL '5' MINUTES)
  )
) R
ON L.num = R.num and L.window_start = R.window_start and L.window_end = R.window_end