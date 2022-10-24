-- 创建Source表
CREATE TABLE KafkaTable (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING,
  `ts` TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'user-behavior',
  'properties.bootstrap.servers' = 'hadoop001:9092,hadoop002:9092,hadoop003:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'csv',
  'csv.ignore-parse-errors' = 'true'
);
-- 创建Sink表(mysql)
CREATE TABLE MyUserTable (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING,
  `ts` TIMESTAMP(3)
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://localhost:3306/doit32',
   'table-name' = 'users',
   'username' = 'root',
   'password' = '123456'
);
-- 查询并写入sink表中
insert into MyUserTable select * from KafkaTable where `behavior` <> 'pay'