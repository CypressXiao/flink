package cn.doitedu.flink.day08;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day08
 * @className: D01_ReadKafkaWriteRedisDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/9/3 9:34
 * @version: 1.0
 */

public class D01_ReadKafkaWriteRedisDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        //设置stateBackend(默认是保存在jobManager内存里)
        //checkpoint到本地文件夹
        //env.setStateBackend(new FsStateBackend("file:///D:\\AllContent\\flink\\flink-java\\data"));
        env.setStateBackend(new FsStateBackend(args[0]));

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop001:9092,hadoop002:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "g001");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        String topic = "doit32";
        List<String> topics = Arrays.asList(topic.split(","));

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topics, new SimpleStringSchema(), props);

        //checkpoint成功,不将偏移量写入到kafka中特殊topic
        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);

        DataStreamSource<String> lines = env.addSource(kafkaConsumer);

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] fields = value.split("\\s+");
                for (String s : fields) {
                    out.collect(Tuple2.of(s, 1));
                }
            }
        }).keyBy(t -> t.f0);

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<Tuple2<String, Integer>> res = windowedStream.aggregate(new MyAggregateFunction(), new MyAggPrecessFunction());



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

    private static class MyAggregateFunction implements AggregateFunction<Tuple2<String, Integer>, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
            return accumulator + value.f1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return null;
        }
    }

    private static class MyAggPrecessFunction extends ProcessWindowFunction<Integer, Tuple2<String, Integer>, String, TimeWindow> {
        ValueStateDescriptor<Integer> descriptor;
        ValueState<Integer> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            descriptor = new ValueStateDescriptor<>("count-state", Integer.class);
            countState = getRuntimeContext().getState(descriptor);
        }

        //窗口触发后,每个key调用一次
        @Override
        public void process(String key, Context context, Iterable<Integer> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
            Integer num = elements.iterator().next();
            Integer count = countState.value();
            if (count == null) {
                count = 0;
            }
            count += num;
            countState.update(count);
            out.collect(Tuple2.of(key, count));
        }
    }

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
