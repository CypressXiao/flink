-- 注册kafka的source表
-- 时间,游戏id,分区id,金额
-- 10000,g1,z1,330.0
create table KafkaTable(
    ts BIGINT,
    gameId String,
    zoneId String,
    money DOUBLE,
    e_time as TO_TIMESTAMP_LTZ(ts,3),
    watermark for e_time as e_time -interval '2' second
)with (
  'connector' = 'kafka',
  'topic' = 'game-log',
  'properties.bootstrap.servers' = 'hadoop001:9092,hadoop002:9092,hadoop003:9092',
  'properties.group.id' = 'g001',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'csv',
  'csv.ignore-parse-errors' = 'true'
);

-- 注册一个printSink表
CREATE TABLE print_table (
    gameId String,
    zoneId String,
    money DOUBLE,
    win_start timestamp(3),
    win_end timestamp(3)
) WITH (
  'connector' = 'print' -- 指定输出的连接器为print sink,将数据在控制台打印
);

-- 查询,并将结果插入到sink表中
-- 按gameId,zoneId和窗口进行group by只统计当前窗口的数据
/*insert into print_table
select gameId,zoneId,sum(money) money,
tumble_start(e_time,INTERVAL '10' SECONDS) win_start,
tumble_end(e_time,INTERVAL '10' SECONDS) win_end
from KafkaTable
group by gameId, zoneId,tumble(e_time,INTERVAL '10' SECONDS)*/

-- TABLE KafkaTable 表名称 DESCRIPTOR(e_time) kafkaTable中有的时间字段,可以是eventTime也可以是processingTime
-- INTERVAL '10' SECONDS 滚动窗口长度
-- window_start, window_end 是TVF语法中的特殊字段

insert into print_table
select gameId,zoneId,sum(money) money,
window_start win_start,-- 名字必须和sink表中的一致
window_end win_end
FROM TABLE(
    HOP(TABLE KafkaTable, DESCRIPTOR(e_time),INTERVAL '5' SECONDS, INTERVAL '10' SECONDS))
GROUP BY window_start, window_end,gameId,zoneId;