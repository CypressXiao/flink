package cn.doitedu.flink.day06;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.*;
import java.util.HashMap;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day06
 * @className: D6_MyKeyedStateDemo1
 * @author: Cypress_Xiao
 * @description: flink的状态就是flink运行时产生的中间结果, 分为
 * keyedState:先keyBy再使用的state,数据和key绑定在一起,有ValueState,MapState,ListState
 * operatorState:没有keyBy,没跟key绑定在一起
 * 用的最多的是keyedState
 * @date: 2022/8/31 15:38
 * @version: 1.0
 */

public class D6_MyKeyedStateDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(4);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000));
        //2.调用Source创建抽象数据集
        //使用nc命令开启一个socket服务
        DataStreamSource<String> source = env.socketTextStream("hadoop001", 8888);
        //3.对抽象数据集进行转换操作
        SingleOutputStreamOperator<String> words = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                if (line.startsWith("error")) {
                    throw new RuntimeException("有错误数据出现,抛出异常!");
                }
                String[] words = line.split("\\s+");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });
        //调用转换算子将数据组成(word,1)的形式
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String w) throws Exception {
                return Tuple2.of(w, 1);
            }
        });

        //按单词进行分区(底层使用的是hash方式分区)
        KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndOne.keyBy(tp -> tp.f0);

        //聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = keyed.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            HashMap<String, Integer> map;
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                map.put(value.f0, map.getOrDefault(value.f0, 0) + value.f1);
                return Tuple2.of(value.f0, map.get(value.f0));
            }
        });

        //调用sink,输出结果
        res.print();

        //启动并执行
        env.execute();

    }
}


