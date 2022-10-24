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
 * @className: FlinkSQLUtils
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/9/11 11:28
 * @version: 1.0
 */

public class FlinkSQLUtils {
    public static void execute(String path) throws IOException {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(3);//当eventTime类型的窗口读取kafka中的数据,要求flink执行环境的并行度小于等于kafka的分区数,否则始终无法生成水位线
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //读取指定文件中的sql内容
        String str = FileUtils.readFileToString(new File(path), "UTF-8");
        String[] sqls = str.split(";");
        for (String sql : sqls) {
            tEnv.executeSql(sql);
        }
    }
}
