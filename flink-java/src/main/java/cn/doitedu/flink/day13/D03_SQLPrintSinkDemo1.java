package cn.doitedu.flink.day13;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.util.Properties;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day13
 * @className: D03_SQLPrintSinkDemo1
 * @author: Cypress_Xiao
 * @description: 将数据输出到指定的Sink中,以后可以输出到Mysql,Kafka等,演示PrintSink将数据输出到控制台
 * @date: 2022/9/11 10:35
 * @version: 1.0
 */

public class D03_SQLPrintSinkDemo1 {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //执行建表的sql(Source的源表)
        tEnv.executeSql("CREATE TABLE KafkaTable (\n" +
                "  `user_id` BIGINT,\n" +
                "  `item_id` BIGINT,\n" +
                "  `behavior` STRING,\n" +
                "  `ts` TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,\n" +
                "  `topic` STRING METADATA VIRTUAL, -- 字段名称和元数据信息名称一样,可以省略from;virtual可写可不写,建议写\n" +
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

        //创建一个sink表
        tEnv.executeSql("CREATE TABLE print_table (\n" +
                "  `user_id` BIGINT,\n" +
                "  `item_id` BIGINT,\n" +
                "  `behavior` STRING,\n" +
                "  `ts` TIMESTAMP(3) ,\n" +
                "  `topic` STRING , \n" +
                "  `partition` BIGINT,\n" +
                "  `offset` BIGINT\n" +
                ") WITH (\n" +
                "  'connector' = 'print' -- 指定输出的连接器为print sink,将数据在控制台打印\n" +
                ")");

        //将source表的数据和sink表的数据建立一座桥梁(关联到一起)
        tEnv.executeSql("insert into print_table select * from KafkaTable where `behavior` <> 'pay' ");


    }
}
