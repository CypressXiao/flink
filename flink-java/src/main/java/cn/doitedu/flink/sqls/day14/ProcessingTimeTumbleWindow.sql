-- 注册kafka的source表
create table KafkaTable(
    gameId String,
    zoneId String,
    money DOUBLE
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
insert into print_table
select gameId,zoneId,sum(money) money,
tumble_start(proctime(),INTERVAL '10' SECONDS) win_start,
tumble_end(proctime(),INTERVAL '10' SECONDS) win_end
from KafkaTable
group by gameId, zoneId,tumble(proctime(),INTERVAL '10' SECONDS)