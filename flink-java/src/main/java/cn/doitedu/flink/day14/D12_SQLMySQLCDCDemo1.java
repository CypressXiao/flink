package cn.doitedu.flink.day14;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day14
 * @className: D12_SQLMySQLCDCDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/9/12 17:04
 * @version: 1.0
 */

public class D12_SQLMySQLCDCDemo1 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql("" +
                "CREATE TABLE mysql_binlog (\n" +
                " id INT primary key,\n" +
                " name STRING\n" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = 'localhost',\n" +
                " 'port' = '3306',\n" +
                " 'username' = 'root',\n" +
                " 'password' = '123456',\n" +
                " 'database-name' = 'doit32',\n" +
                " 'table-name' = 'goods_dim'\n" +
                ")");


        TableResult tableResult = tEnv.executeSql("select * from mysql_binlog");

        tableResult.print();
    }
}
