-- flink SQL 支持按照条件窗口cube
-- cube包含所有条件的组合，grouping sets 是可以选择指定哪些维度
-- 按照 商品分类ID、品牌ID创建cube
CREATE TABLE tb_events (
  `pid` string, --商品ID
  `cid` string , -- 商品分类ID
  `brand` string, -- 品牌ID
  `money` DOUBLE , -- 金额
  `ts` as PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'game-log',
  'properties.bootstrap.servers' = 'hadoop001:9092,hadoop002:9092,hadoop003:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'csv',
  'csv.ignore-parse-errors' = 'true'
  )
;

-- MySQL sink表
CREATE TABLE tb_result (
  `cid` string ,
  `brand` string,
  `money` DOUBLE,
  PRIMARY KEY (cid, brand) NOT ENFORCED -- 将cid和品牌作为联合主键
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://localhost:3306/doit32?characterEncoding=utf-8',
  'table-name' = 'tb_cube', -- 写入的mysql数据库对应的表
  'username' = 'root',
  'password' = '123456'
  )
;

-- 按照cid和brand创建cube
insert into tb_result
select
  IFNULL(cid, 'NULL') cid,
  IFNULL(brand, 'NULL') brand,
  sum(money) money
from
  tb_events
group by cube(cid, brand)
;

--以后在mysql中根据相应的条件进行查询
-- select money from tb_cube where cid = 'NULL' and brand = 'NULL'
-- select money from tb_cube where cid = 'phone' and brand = 'NULL'
-- select money from tb_cube where cid = 'NULL' and brand = 'huawei'
-- select money from tb_cube where cid = 'phone' and brand = 'huawei'