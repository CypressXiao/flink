package cn.doitedu.flink.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day03
 * @className: D3_SumDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/8/27 10:49
 * @version: 1.0
 */

public class D3_SumDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(4);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split("\\s+");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));

            }
        });
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tpStream.keyBy(tp -> tp.f0);
        //SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum("f1");
        sum.print();
        env.execute();


    }
}
