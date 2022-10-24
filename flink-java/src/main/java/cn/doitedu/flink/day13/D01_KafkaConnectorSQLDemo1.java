package cn.doitedu.flink.day13;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day13
 * @className: D01_KafkaConnectorSQLDemo1
 * @author: Cypress_Xiao
 * @description: 使用SQL的方式对kafka中的数据进行处理, 数据已经在kafka中, 并且是结构化数据(例如csv, json格式)
 * 然后使用FlinkSQL的命令创建表(指定Schema信息:字段名称,类型,数据从哪里读等),该例子演示读取csv格式的数据
 * @date: 2022/9/11 9:13
 * @version: 1.0
 */

public class D01_KafkaConnectorSQLDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //执行建表的sql
        tEnv.executeSql("CREATE TABLE KafkaTable (\n" +
                "  `user_id` BIGINT,\n" +
                "  `item_id` BIGINT,\n" +
                "  `behavior` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'user-behavior',\n" +
                "  'properties.bootstrap.servers' = 'hadoop001:9092,hadoop002:9092,hadoop003:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'csv',\n" +
                "  'csv.ignore-parse-errors' = 'true' -- 忽略解析出错的数据 \n" +
                ")");

        TableResult tableResult = tEnv.executeSql("select * from KafkaTable where `behavior` <> 'pay' ");
        tableResult.print();
        //没有使用dataStream就不用下句命令
        //env.execute();


    }
}
