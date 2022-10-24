package cn.doitedu.flink.day07;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day07
 * @className: D06_WindowAggWithHistoryStateDemo1
 * @author: Cypress_Xiao
 * @description:
 * 将窗口内的数据进行增量聚合,然后再与历史数据聚合
 * 使用场景,将数据进行累加写入到外部的数据库中
 * @date: 2022/9/2 16:04
 * @version: 1.0
 */

public class D06_WindowAggWithHistoryStateDemo1 {
    public static void main(String[] args) throws Exception {

        //1.创建flink的执行环境(StreamExecutionEnvironment相当于Spark中的SparkContext)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.调用Source创建抽象数据集
        //使用nc命令开启一个socket服务
        DataStreamSource<String> source = env.socketTextStream("hadoop001", 8888);
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

        FlinkJedisPoolConfig redisConf = new FlinkJedisPoolConfig
                .Builder()
                .setHost("hadoop001")
                .setPort(6379)
                .setDatabase(0)
                .build();

        res.addSink(new RedisSink<>(redisConf,new RedisWordCountMapper()));

        env.execute();
    }


    //将数据以何种写入到Redis中的数据进行映射,哪个作为key,哪个是value
    private static class RedisWordCountMapper implements RedisMapper<Tuple2<String,Integer>>{
        @Override
        public RedisCommandDescription getCommandDescription() {
            //RedisCommand.HSET 调用redis的Hset方法写入
            return new RedisCommandDescription(RedisCommand.HSET, "WC");
        }


        //将数据中的哪个字段作为key
        @Override
        public String getKeyFromData(Tuple2<String, Integer> data) {
            return data.f0;
        }
        //将数据中的哪个字段作为value
        @Override
        public String getValueFromData(Tuple2<String, Integer> data) {
            return data.f1.toString();
        }
    }
}
