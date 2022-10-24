package cn.doitedu.flink.day12;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day12
 * @className: D06_SqlEventTimeTumblingWindowDemo1
 * @author: Cypress_Xiao
 * @description: 使用eventTime划分滚动窗口
 * @date: 2022/9/9 15:05
 * @version: 1.0
 */

public class D06_SqlEventTimeTumblingWindowDemo1 {
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

        tEnv.createTemporaryView("v_aid_log",tpStream,$("uid"),$("eid"),$("aid"),$("e_time").rowtime());

        //使用FlinkSQL实现滚动窗口
        //Tumble(e_time,INTERVAL '10' seconds)按照指定的时间段划分窗口,并指定窗口类型和窗口的长度
        TableResult tableResult = tEnv.executeSql("select aid,eid,count(*) as counts,tumble_start(e_time,INTERVAL '10' SECONDS) as win_start,tumble_end(e_time,INTERVAL '10' SECONDS) as win_end from v_aid_log group by tumble(e_time,INTERVAL '10' SECONDS),aid,eid");

        tableResult.print();

        env.execute();


    }
}
