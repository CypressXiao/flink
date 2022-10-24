package cn.doitedu.flink.day12;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day12
 * @className: PracticeDemo2
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/9/9 10:42
 * @version: 1.0
 */

public class PracticeDemo2 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8081);
        Random ran = new Random();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        SingleOutputStreamOperator<Tuple4<String, String, Double, Integer>> tpStream = lines.map(new MapFunction<String, Tuple4<String, String, Double, Integer>>() {
            @Override
            public Tuple4<String, String, Double, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple4.of(fields[0], fields[1], Double.parseDouble(fields[2]), ran.nextInt(4));
            }
        });


        KeyedStream<Tuple4<String, String, Double, Integer>, Tuple3<String, String, Integer>> keyedStream = tpStream.keyBy(t -> Tuple3.of(t.f0, t.f1, t.f3));

        WindowedStream<Tuple4<String, String, Double, Integer>, Tuple3<String, String, Integer>, TimeWindow> windowedStream = keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)));

        windowedStream.aggregate(new MyAggFunction(),new MyProcessFunction());

    }
    private static class MyAggFunction implements AggregateFunction<Tuple4<String, String, Double, Integer>,Tuple3<String,String,Double>,Tuple3<String,String,Double>>{
        @Override
        public Tuple3<String, String, Double> createAccumulator() {
            return new Tuple3<>();
        }

        @Override
        public Tuple3<String, String, Double> add(Tuple4<String, String, Double, Integer> value, Tuple3<String, String, Double> accumulator) {
            accumulator.f2 += value.f2;
            return accumulator;
        }

        @Override
        public Tuple3<String, String, Double> getResult(Tuple3<String, String, Double> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple3<String, String, Double> merge(Tuple3<String, String, Double> a, Tuple3<String, String, Double> b) {
            return null;
        }
    }

    private static class MyProcessFunction extends ProcessWindowFunction< Tuple3<String, String, Double>, Tuple3<String, String, Double>, Tuple3<String, String, Integer>,TimeWindow>{
        @Override
        public void process(Tuple3<String, String, Integer> stringStringIntegerTuple3, Context context, Iterable<Tuple3<String, String, Double>> elements, Collector<Tuple3<String, String, Double>> out) throws Exception {

        }
    }
}
