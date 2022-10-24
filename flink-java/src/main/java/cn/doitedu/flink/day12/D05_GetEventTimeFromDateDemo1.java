package cn.doitedu.flink.day12;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day12
 * @className: D05_GetEventTimeFromDateDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/9/9 14:36
 * @version: 1.0
 */

public class D05_GetEventTimeFromDateDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        //1000,uid,eid,aid
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);

        //提取eventTime生成水位线
        SingleOutputStreamOperator<String> watermarks = lines.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String element, long recordTimestamp) {
                String[] fields = element.split(",");
                return Long.parseLong(fields[0]);
            }
        }));

        SingleOutputStreamOperator<Tuple3<String, String, String>> tpStream = watermarks.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[1], fields[2], fields[3]);
            }
        });

        //注册视图
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //使用新api
        //rowtime获取的是eventTime
        /*Schema schema = Schema.newBuilder()
                .columnByExpression("uid", $("f0"))
                .columnByExpression("eid", $("f1"))
                .columnByExpression("aid", $("f2"))
                .columnByMetadata("e_time", DataTypes.TIMESTAMP_LTZ(3), "rowtime")  //拿出eventTime
                .build();
*/
        //注册视图
        tEnv.createTemporaryView("v_aid_log", tpStream, $("uid"), $("eid"), $("aid"), $("e_time").rowtime());

        //TableResult tableResult = tEnv.executeSql("desc v_aid_log");
        TableResult tableResult = tEnv.executeSql("select *,current_watermark(e_time) `watermark` from v_aid_log");

        tableResult.print();

        env.execute();

    }
}
