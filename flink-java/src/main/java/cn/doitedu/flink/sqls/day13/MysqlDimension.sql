-- 表时sink表还是source表,是看你在转化sql里面的是怎么写的

-- 创建一个source表
create table Kafka_events(
    oid String,
    cid BIGINT,
    money DOUBLE,
    `ts` as proctime()
)with (
  'connector' = 'kafka',
  'topic' = 'event-log',
  'properties.bootstrap.servers' = 'hadoop001:9092',
  'properties.group.id' = 'g001',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'csv',
  'csv.ignore-parse-errors' = 'true'
);

-- 创建mysql维表
CREATE TABLE tb_dimension (
   id BIGINT,
   `name` String
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://localhost:3306/doit32',
   'table-name' = 'goods_dim',
   'username' = 'root',
   'password' = '123456',
   'lookup.cache.max-rows'='1000', -- 缓存最大的行数
   'lookup.cache.ttl'='2min' -- 缓存数据时长
);

-- 创建一个sink表
CREATE TABLE print_event (
    oid String,
    cid BIGINT,
    money DOUBLE,
    `name` String
) WITH (
 'connector' = 'print'
);

insert into print_event
SELECT  oid,Kafka_events.cid,money,tb_dimension.name FROM Kafka_events
LEFT JOIN tb_dimension FOR SYSTEM_TIME AS OF Kafka_events.ts
ON Kafka_events.cid = tb_dimension.id;

