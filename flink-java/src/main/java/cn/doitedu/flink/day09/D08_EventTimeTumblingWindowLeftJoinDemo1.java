package cn.doitedu.flink.day09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day09
 * @className: D07_EventTimeTumblingWindowDemo1
 * @author: Cypress_Xiao
 * @description: 两流join并且按照eventTime划分窗口,实现左外连接
 * @date: 2022/9/5 16:07
 * @version: 1.0
 */

public class D08_EventTimeTumblingWindowLeftJoinDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8081);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //1000,o101,c10,3000
        //1000,o102,c11,3000
        DataStreamSource<String> lines1 = env.socketTextStream("hadoop001", 8888);
        //1111,c10,图书
        //1222,c11,服装
        DataStreamSource<String> lines2 = env.socketTextStream("hadoop001", 9999);

        SingleOutputStreamOperator<String> watermarks1 = lines1.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String element, long recordTimestamp) {
                return Long.parseLong(element.split(",")[0]);
            }
        }));

        SingleOutputStreamOperator<String> watermarks2 = lines2.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String element, long recordTimestamp) {
                return Long.parseLong(element.split(",")[0]);
            }
        }));

        SingleOutputStreamOperator<Tuple3<String, String, Double>> tpStream1 = watermarks1.map(new MapFunction<String, Tuple3<String, String, Double>>(
        ) {
            @Override
            public Tuple3<String, String, Double> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[1], fields[2], Double.parseDouble(fields[3]));
            }
        });

        SingleOutputStreamOperator<Tuple2<String, String>> tpStream2 = watermarks2.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[1], fields[2]);
            }
        });

        DataStream<Tuple4<String, String, Double, String>> res = tpStream1.coGroup(tpStream2)
                .where(t -> t.f1)
                .equalTo(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<Tuple3<String, String, Double>, Tuple2<String, String>, Tuple4<String, String, Double, String>>() {
                    @Override
                    public void coGroup(Iterable<Tuple3<String, String, Double>> first, Iterable<Tuple2<String, String>> second, Collector<Tuple4<String, String, Double, String>> out) throws Exception {
                        boolean flag = false;
                        for (Tuple3<String, String, Double> tp1 : first) {
                            for (Tuple2<String, String> tp2 : second) {
                                out.collect(Tuple4.of(tp1.f0, tp1.f1, tp1.f2, tp2.f1));
                                flag = true;
                            }
                            if (!flag) {
                                out.collect(Tuple4.of(tp1.f0, tp1.f1, tp1.f2, null));
                            }
                        }
                    }
                });

        res.print();
        env.execute();

    }
}
