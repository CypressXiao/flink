//创建source表
create table KafkaTable(
    gameId String,
    zoneId String,
    money DOUBLE,
    `ts` as proctime() -- 通过函数取出processTime
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
`rank`
from
(select
    gameId,
    zoneId,
    money,
    row_number() over(partition by gameId order by money desc) `rank`
from
(
  select
    gameId,
    zoneId,
    sum(money) money
    from KafkaTable
    group by gameId, zoneId)
)
where `rank`<=3;
//将视图中的数据插入
insert into MyUserTable
select * from temp_view