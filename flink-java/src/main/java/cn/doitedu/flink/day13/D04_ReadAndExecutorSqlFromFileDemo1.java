package cn.doitedu.flink.day13;

import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;
import java.io.IOException;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day13
 * @className: D04_ReadAndExecutorSqlFromFileDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/9/11 11:13
 * @version: 1.0
 */

public class D04_ReadAndExecutorSqlFromFileDemo1 {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //读取指定文件中的sql内容
        String str = FileUtils.readFileToString(new File(args[0]), "UTF-8");
        String[] sqls = str.split(";");
        for (String sql : sqls) {
            tEnv.executeSql(sql);
        }
    }
}
