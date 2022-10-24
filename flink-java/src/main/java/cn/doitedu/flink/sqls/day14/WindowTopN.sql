//创建source表
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
//创建sink表
CREATE TABLE MyUserTable (
   gameId String,
   zoneId String,
   money Double,
   window_start timestamp(3) ,
   window_end timestamp(3),
   `rank` BIGINT
) WITH (
   'connector' = 'print'
);

//将查询到的数据插入到print表中

//创建临时视图
create temporary view temp_view as
select
gameId,
zoneId,
money,
window_start,
window_end,
`rank`
from
(select
    gameId,
    zoneId,
    money,
    window_start,
    window_end,
    row_number() over(partition by window_start,window_end,gameId order by money desc) `rank`
from
  (
  select
    gameId,
    zoneId,
    sum(money) money,
    window_start,
    window_end
    FROM TABLE(
        TUMBLE(TABLE KafkaTable, DESCRIPTOR(e_time), INTERVAL '20' SECOND))
    GROUP BY window_start, window_end, gameId,zoneId
    )
)
where `rank`<=3;
//将视图中的数据插入
insert into MyUserTable
select * from temp_view