package cn.doitedu.flink.day13;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day13
 * @className: D07_FlinkSQLWatermarkDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/9/11 17:03
 * @version: 1.0
 */

public class D07_FlinkSQLWatermarkDemo1 {
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
                "  `ts` TIMESTAMP(3) ,\n" +
                "  WATERMARK FOR ts AS ts - INTERVAL '2' SECOND -- 使用ts字段作为eventTime并生成watermark \n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'user-behavior-json',\n" +
                "  'properties.bootstrap.servers' = 'hadoop001:9092,hadoop002:9092,hadoop003:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'json.ignore-parse-errors' = 'true'\n" +
                ")");
        //TableResult tableResult = tEnv.executeSql("desc KafkaTable");
        TableResult tableResult = tEnv.executeSql("select *,current_watermark(ts) from KafkaTable");
        tableResult.print();

    }
}
