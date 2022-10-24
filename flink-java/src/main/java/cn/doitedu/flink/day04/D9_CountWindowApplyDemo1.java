package cn.doitedu.flink.day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day04
 * @className: D9_CountWindowApplyDemo1
 * @author: Cypress_Xiao
 * @description: 对窗口中的数据进行全量操作
 * @date: 2022/8/28 16:42
 * @version: 1.0
 */

public class D9_CountWindowApplyDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> tp = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tp.keyBy(t -> t.f0);
        //先keyBy再划分window当一个组内数据达到指定的条数,这个组的数据单独触发
        WindowedStream<Tuple2<String, Integer>, String, GlobalWindow> windowedStream = keyedStream.countWindow(5);
        //对窗口内的数据进行排序,apply是对窗口进行全量的运算
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = windowedStream.apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, GlobalWindow>() {
            /**
             * @param s: keyBy字段
             * @param window: 窗口类型
             * @param input: 输入的数据,先将其攒起来,放在一个集合里
             * @param out: 输出的数据
             * @return void
             * @author Cypress_Xiao
             * @description 当窗口触发, 每个key会调用一次apply方法
             * @date 2022/8/28 16:44
             */
            @Override
            public void apply(String s, GlobalWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
                ArrayList<Tuple2<String,Integer>> list = (ArrayList<Tuple2<String,Integer>>) input;
                list.sort(new Comparator<Tuple2<String, Integer>>() {
                    @Override
                    public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
                        return o2.f1 - o1.f1;
                    }
                });
                for (Tuple2<String, Integer> tp : list) {
                    out.collect(tp);
                }
            }
        });
        res.print();
        env.execute();

    }
}
