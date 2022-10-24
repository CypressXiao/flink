package cn.doitedu.flink.day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day04
 * @className: D7_CountWindowDemo1
 * @author: Cypress_Xiao
 * @description: 先keyBy然后再划分window,属于keyedWindow,是多并行的
 * @date: 2022/8/28 15:58
 * @version: 1.0
 */

public class D7_CountWindowDemo1 {
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
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = windowedStream.sum(1);
        res.print();
        env.execute();

    }
}
