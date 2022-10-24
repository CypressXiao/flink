package cn.doitedu.flink.day11;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;


/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day11
 * @className: D08_SQLAdCountDemo2
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/9/8 21:35
 * @version: 1.0
 */

public class D08_SQLAdCountDemo2 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        //为了容错，必须开启checkpoint
        env.enableCheckpointing(15000);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, String>> tpStream = lines.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], fields[2]);
            }
        });

        //注册视图
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Schema schema = Schema.newBuilder()
                .columnByExpression("uid", $("f0"))
                .columnByExpression("eid", $("f1"))
                .columnByExpression("aid", $("f2"))
                .build();

        tEnv.createTemporaryView("v_ad_log", tpStream, schema);

        TableResult tableResult = tEnv.executeSql("desc v_ad_log");

        //TableResult tableResult = tEnv.executeSql("select aid, eid, count(*) counts, count(distinct(uid)) dis_counts from v_ad_log group by aid, eid");

        tableResult.print();

        env.execute();

    }
}
