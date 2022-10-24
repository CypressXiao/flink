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
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'csv',
  'csv.ignore-parse-errors' = 'true'
);
//创建sink表
CREATE TABLE MyUserTable (
   gameId String,
   zoneId String,
   total Double,
  primary key (gameId,zoneId) NOT ENFORCED
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://localhost:3306/doit32',
   'table-name' = 'gameCount',
   'username' = 'root',
   'password' = '123456'
);
//将查询到的数据插入到mysql中
insert into MyUserTable
select
  gameId,
  zoneId,
  sum(money) total
from
(
  select
    gameId,
    zoneId,
    sum(money) money
  from
  (
    select
      ts,
      gameId,
      zoneId,
      RAND_INTEGER(4) ri, -- 生成[0, 4)的随机数
      money
    from KafkaTable
  ) group by gameId, zoneId, ri, tumble(ts, interval '10' second)
) group by gameId, zoneId