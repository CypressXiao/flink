package cn.doitedu.flink.day05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day05
 * @className: D3_KeyedProcessingTimeSessionWindowDemo1
 * @author: Cypress_Xiao
 * @description:
 * ProcessingTimeSessionWindow触发机制是:当前系统时间 - 进入到每个组最后一条数据的时间 > 指定的时间间隔
 * (满足条件的组内数据会单独触发)
 * @date: 2022/8/30 11:21
 * @version: 1.0
 */

public class D4_KeyedProcessingTimeSessionWindowDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tpStream.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)));

        SingleOutputStreamOperator<Tuple2<String, Integer>> res = windowedStream.reduce((ReduceFunction<Tuple2<String, Integer>>) (value1, value2) -> {
            //不能使用之前给value1重赋值的方式
            int count = value1.f1+value2.f1;
            return Tuple2.of(value1.f0,count);
        });

        res.print();

        env.execute();
    }
}
