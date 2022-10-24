package cn.doitedu.flink.day09;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day09
 * @className: D07_ProcessingTimeTumblingWindowDemo1
 * @author: Cypress_Xiao
 * @description: 将两个流按照相同的条件进行keyBy(在同一台机器的同一个分区内), 并且划分滚动窗口（在同一个时间段内),进行join
 * @date: 2022/9/5 15:44
 * @version: 1.0
 */

public class D07_ProcessingTimeTumblingWindowJoinDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8081);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<String> lines1 = env.socketTextStream("hadoop001", 8888);
        DataStreamSource<String> lines2 = env.socketTextStream("hadoop001", 9999);

        SingleOutputStreamOperator<Tuple3<String, String, Double>> tpStream1 = lines1.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], Double.parseDouble(fields[2]));
            }
        });

        SingleOutputStreamOperator<Tuple2<String, String>> tpStream2 = lines2.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], fields[1]);
            }
        });

        DataStream<Tuple4<String, String, Double, String>> res = tpStream1.join(tpStream2)
                .where(t -> t.f1)   //第一个流的join条件
                .equalTo(t -> t.f0) //第二个流的join条件
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30))) //将两流的数据放入同一个窗口,进行全量计算
                .apply(new JoinFunction<Tuple3<String, String, Double>, Tuple2<String, String>, Tuple4<String, String, Double, String>>() {

                    //窗口触发后相同key的数据在同一个窗口内join上了才会调用join方法,说明是包含相同字段的
                    @Override
                    public Tuple4<String, String, Double, String> join(Tuple3<String, String, Double> t1, Tuple2<String, String> t2) throws Exception {
                        return Tuple4.of(t1.f0, t1.f1, t1.f2, t2.f1);
                    }
                });

        res.print();
        env.execute();


    }
}
