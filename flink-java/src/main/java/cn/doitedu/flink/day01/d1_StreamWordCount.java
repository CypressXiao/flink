package cn.doitedu.flink.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day01
 * @className: d1_StreamWordCount
 * @author: Cypress_Xiao
 * @description: flink的wordCount
 * @date: 2022/8/24 11:14
 * @version: 1.0
 */

public class d1_StreamWordCount {
    public static void main(String[] args) throws Exception {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        //1.创建flink的执行环境(StreamExecutionEnvironment相当于Spark中的SparkContext)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.调用Source创建抽象数据集
        //使用nc命令开启一个socket服务
        DataStreamSource<String> source = env.socketTextStream(host, port);
        //3.对抽象数据集进行转换操作
        SingleOutputStreamOperator<String> words = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
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
        KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tp) throws Exception {
                return tp.f0;
            }
        });

        //聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = keyed.sum(1);

        //调用sink,输出结果
        res.print();

        //启动并执行
        env.execute();

    }
}
