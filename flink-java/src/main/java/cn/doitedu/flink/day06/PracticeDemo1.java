package cn.doitedu.flink.day06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day06
 * @className: PracticeDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/8/31 9:10
 * @version: 1.0
 */

/**
 * 1000,o001,一区,3000
 * 2000,o002,二区,3000
 * 3000,o003,二区,6000
 * 4000,o004,三区,7000
 * 5000,o005,二区,1000
 * 6000,o006,二区,5000
 * 7000,o007,二区,2000
 * 8000,o008,三区,9000
 * 9000,o009,二区,500
 * 9000,o010,二区,1000
 * 10000,o011,一区,8000
 * 11000,o011,一区,7000
 * 12000,o011,一区,9000
 */
public class PracticeDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);

        SingleOutputStreamOperator<String> watermarks = lines.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String element) {
                String[] fields = element.split(",");
                return Long.parseLong(fields[0]);
            }
        });

        SingleOutputStreamOperator<Tuple4<Long, String, String, Double>> tpStream = watermarks.map(new MapFunction<String, Tuple4<Long, String, String, Double>>() {
            @Override
            public Tuple4<Long, String, String, Double> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple4.of(Long.parseLong(fields[0]), fields[1], fields[2], Double.parseDouble(fields[3]));
            }
        });


        KeyedStream<Tuple4<Long, String, String, Double>, String> keyedStream = tpStream.keyBy(new KeySelector<Tuple4<Long, String, String, Double>, String>() {
            @Override
            public String getKey(Tuple4<Long, String, String, Double> value) throws Exception {
                return value.f2;
            }
        });

        WindowedStream<Tuple4<Long, String, String, Double>, String, TimeWindow> window = keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)));

        /*SingleOutputStreamOperator<Tuple3<String, String, Double>> res = window.apply(new WindowFunction<Tuple4<Long, String, String, Double>, Tuple3<String, String, Double>, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<Tuple4<Long, String, String, Double>> input, Collector<Tuple3<String, String, Double>> out) throws Exception {
                ArrayList<Tuple4<Long, String, String, Double>> list = (ArrayList<Tuple4<Long, String, String, Double>>) input;
                list.sort(new Comparator<Tuple4<Long, String, String, Double>>() {
                    @Override
                    public int compare(Tuple4<Long, String, String, Double> o1, Tuple4<Long, String, String, Double> o2) {
                        return (int) (o2.f3 - o1.f3);
                    }
                });
                for (int i = 0; i <Math.min(list.size(),3); i++) {
                    Tuple4<Long, String, String, Double> tp = list.get(i);
                    out.collect(Tuple3.of(tp.f2,tp.f1,tp.f3));
                }
            }
        });*/
        SingleOutputStreamOperator<Tuple3<String, String, Double>> res = window.process(new MyProcessFunction());

        res.print();
        env.execute();
    }
}

/**
 * 自定义UDF函数
 */
class MyProcessFunction extends ProcessWindowFunction<Tuple4<Long,String,String,Double>,Tuple3<String,String,Double>,String,TimeWindow>{
    /**
     * 当窗口触发,在窗口内出现的每个key都会调用一次process方法
     * @param s:
     * @param context:
     * @param elements:
     * @param out:
     * @throws Exception
     */
    @Override
    public void process(String s, Context context, Iterable<Tuple4<Long, String, String, Double>> elements, Collector<Tuple3<String, String, Double>> out) throws Exception {
        ArrayList<Tuple4<Long, String, String, Double>> list = (ArrayList<Tuple4<Long, String, String, Double>>) elements;
        list.sort((o1, o2) -> Double.compare(o2.f3,o1.f3));
        for (int i = 0; i <Math.min(list.size(),3); i++) {
            Tuple4<Long, String, String, Double> tp = list.get(i);
            out.collect(Tuple3.of(tp.f2,tp.f1,tp.f3));
        }
    }
}
