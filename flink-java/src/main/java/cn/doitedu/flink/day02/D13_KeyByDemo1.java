package cn.doitedu.flink.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day02
 * @className: D13_KeyByDemo1
 * @author: Cypress_Xiao
 * @description: keyBy按照指定的key进行分区
 * @date: 2022/8/25 16:08
 * @version: 1.0
 */

public class D13_KeyByDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        //对数据进行压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String s : value.split("\\s+")) {
                    out.collect(Tuple2.of(s, 1));
                }
            }
        });
        //对数据进行分区
        /*KeyedStream<Tuple2<String, Integer>, String> keyedStream = tpStream.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });
        */
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tpStream.keyBy(tp -> tp.f0);

        keyedStream.print();
        env.execute();
    }
}
