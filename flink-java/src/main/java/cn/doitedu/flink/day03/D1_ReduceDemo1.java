package cn.doitedu.flink.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.util.Properties;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day03
 * @className: D1_ReduceDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/8/27 9:28
 * @version: 1.0
 */

public class D1_ReduceDemo1 {
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
        //reduce,max等聚合算子只能在keyedStream上调用
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduceStream = keyedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            /**
             * 传入的两个参数一定是key相同的
             * @param value1: 初始值或者中间累加的结果
             * @param value2: 相同key的其他数据
             * @return Tuple2<String, Integer>
             * @author Cypress_Xiao
             * @description TODO
             * @date 2022/8/27 9:40
             */
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                value1.f1 = value1.f1 + value2.f1;
                return value1;
            }
        });

        reduceStream.print();
        env.execute();
    }
}
