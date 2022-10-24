//创建Source表
CREATE TABLE KafkaTable (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'user-behavior',
  'properties.bootstrap.servers' = 'hadoop001:9092,hadoop002:9092,hadoop003:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'csv',
  'csv.ignore-parse-errors' = 'true'
);
//创建sink表
CREATE TABLE MyUserTable (
  `user_id` BIGINT,
  `behavior` STRING,
  `count` BIGINT,
  primary key (`user_id`,`behavior`) NOT ENFORCED
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://localhost:3306/doit32',
   'table-name' = 'userCount',
   'username' = 'root',
   'password' = '123456'
);
//将查询到的数据插入到mysql中
insert into MyUserTable select `user_id`,`behavior`,count(1) as `count` from KafkaTable group by `user_id`,`behavior`