package cn.doitedu.flink.day07;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day07
 * @className: D06_WindowAggWithHistoryStateDemo2
 * @author: Cypress_Xiao
 * @description: 改进:先将数据在窗口内进行增量累加,然后写入到外部的数据库,缺点就是实时性降低
 * @date: 2022/9/2 16:40
 * @version: 1.0
 */

public class D07_WindowAggWithHistoryStateDemo2 {
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

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyed.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));
        //传入一个窗口内聚合的逻辑,再传入一个窗口间的聚合逻辑
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = windowedStream.reduce(new MyReduceFunction(), new MyProcessFunction());


        //聚合

        FlinkJedisPoolConfig redisConf = new FlinkJedisPoolConfig
                .Builder()
                .setHost("hadoop001")
                //redis的端口号
                .setPort(6379)
                //redis中的数据库是0-15,默认是0
                .setDatabase(0)
                .build();

        res.addSink(new RedisSink<>(redisConf, new RedisWordCountMapper()));

        env.execute();
    }

    private static class MyReduceFunction implements ReduceFunction<Tuple2<String, Integer>> {
        //当前窗口内的相同key的数据第一次出现后,每来一次调一次
        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
            value1.f1 = value1.f1 + value2.f1;
            return value1;
        }
    }

    private static class MyProcessFunction extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {
        ValueStateDescriptor<Integer> descriptor;
        ValueState<Integer> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            descriptor = new ValueStateDescriptor<>("count-state", Integer.class);
            countState = getRuntimeContext().getState(descriptor);
        }

        //窗口触发后,每个key调用一次
        @Override
        public void process(String s, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
            Tuple2<String, Integer> tp = elements.iterator().next();
            Integer count = countState.value();
            if (count == null) {
                count = 0;
            }
            count += tp.f1;
            countState.update(count);
            tp.f1 = count;
            out.collect(tp);
        }
    }


    //将数据以何种写入到Redis中的数据进行映射,哪个作为key,哪个是value
    private static class RedisWordCountMapper implements RedisMapper<Tuple2<String, Integer>> {
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
