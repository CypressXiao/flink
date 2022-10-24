//创建Source表
CREATE TABLE KafkaTable (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING,
  `ts` TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,
  `topic` STRING METADATA VIRTUAL, -- 字段名称和元数据信息名称一样,可以省略from,virtual可写可不写,建议写
  `partition` BIGINT METADATA VIRTUAL,
  `offset` BIGINT METADATA VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'user-behavior-json',
  'properties.bootstrap.servers' = 'hadoop001:9092,hadoop002:9092,hadoop003:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);
//创建sink表
CREATE TABLE print_table (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING,
  `ts` TIMESTAMP(3) ,
  `topic` STRING ,
  `partition` BIGINT,
  `offset` BIGINT
) WITH (
  'connector' = 'print' -- 指定输出的连接器为print sink,将数据在控制台打印
);
//将source表中的数据插入到sink表中
insert into print_table select * from KafkaTable where `behavior` <> 'pay'