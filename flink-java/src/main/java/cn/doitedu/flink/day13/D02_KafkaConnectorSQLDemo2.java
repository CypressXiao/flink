package cn.doitedu.flink.day13;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day13
 * @className: D02_KafkaConnectorSQLDemo2
 * @author: Cypress_Xiao
 * @description:
 * @date: 2022/9/11 10:10
 * @version: 1.0
 */

public class D02_KafkaConnectorSQLDemo2 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //执行建表的sql
        tEnv.executeSql("CREATE TABLE KafkaTable (\n" +
                "  `user_id` BIGINT,\n" +
                "  `item_id` BIGINT,\n" +
                "  `behavior` STRING,\n" +
                "  `ts` TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,\n" +
                "  `topic` STRING METADATA VIRTUAL, --字段名称和元数据信息名称一样,可以省略from,virtual可写可不写,建议写\n" +
                "  `partition` BIGINT METADATA VIRTUAL,\n" +
                "  `offset` BIGINT METADATA VIRTUAL\n" +
                "  \n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'user-behavior-json',\n" +
                "  'properties.bootstrap.servers' = 'hadoop001:9092,hadoop002:9092,hadoop003:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'json.ignore-parse-errors' = 'true'\n" +
                ")");

        TableResult tableResult = tEnv.executeSql("select user_id,item_id from KafkaTable where `behavior` <> 'pay' ");
        tableResult.print();

    }
}
