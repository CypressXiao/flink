package cn.doitedu.flink.day07;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
 * @className: D08_WindowAggWithHistoryStateDemo3
 * @author: Cypress_Xiao
 * @description: 使用reduce方法必须要求输入和返回值数据类型一致,现在使用aggregate方法,传入的参数和返回值类型可以不一致
 * @date: 2022/9/2 17:22
 * @version: 1.0
 */

public class D08_WindowAggWithHistoryStateDemo3 {
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
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = windowedStream.aggregate(
                new MyAggregateFunction()//返回Integer类型
                , new MyAggProcessFunction());//输入的是Integer类型


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

    private static class MyAggregateFunction implements AggregateFunction<Tuple2<String, Integer>, Integer, Integer> {
        //创建每个窗口内每个key的初始值
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        //每个key的数据输出一次,就会调用一次add方法
        @Override
        public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
            return accumulator+value.f1; //将输入单词的次数与初始值或者历史数据进行累加
        }

        //窗口触发后返回对应key累加的结果
        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        //只有会话窗口才会调用merge方法,其他类型的窗口不会调用
        @Override
        public Integer merge(Integer a, Integer b) {
            return null;
        }
    }


    private static class MyAggProcessFunction extends ProcessWindowFunction<Integer, Tuple2<String, Integer>, String, TimeWindow> {
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
            out.collect(Tuple2.of(key,count));
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