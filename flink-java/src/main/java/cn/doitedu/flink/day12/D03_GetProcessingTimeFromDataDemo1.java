package cn.doitedu.flink.day12;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day12
 * @className: D03_GetProcessingTimeFromDataDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/9/9 11:22
 * @version: 1.0
 */

public class D03_GetProcessingTimeFromDataDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);


        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, String>> tpStream = lines.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], fields[2]);
            }
        });

        //注册视图
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //增加一个当前processTime的字段
        tEnv.createTemporaryView("v_aid_log",tpStream,$("uid"),$("eid"),$("aid"),$("p_time").proctime());

        //TableResult tableResult = tEnv.executeSql("desc v_aid_log");
        TableResult tableResult = tEnv.executeSql("select * from v_aid_log");

        tableResult.print();;
        env.execute();
    }
}
