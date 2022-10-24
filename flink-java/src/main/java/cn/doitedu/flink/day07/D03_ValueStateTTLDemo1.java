package cn.doitedu.flink.day07;

import cn.doitedu.flink.day06.D7_FlinkKeyedStateDemo1;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day07
 * @className: D03_ValueStateTTLDemo1
 * @author: Cypress_Xiao
 * @description: 只有keyedState可以设置TTL(TimeToLive,即数据存在时间),valueStateTTL是对value设置TTL
 * @date: 2022/9/2 14:38
 * @version: 1.0
 */

public class D03_ValueStateTTLDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(4);
        env.enableCheckpointing(5000);
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

        SingleOutputStreamOperator<Tuple2<String, Integer>> res = keyed.map(new MyTTLWcValueState());

        res.print();

        env.execute();


    }

    public static class MyTTLWcValueState extends RichMapFunction<Tuple2<String, Integer>,Tuple2<String, Integer>> {
        private ValueState<Integer> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //设置TTL的配置
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(10))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    // [默认的]设置ttl时间更新方式,当窗口创建该key或更新该key对应的value最近修改时间,会重新计时
                    //.setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite) //除了创建,修改外,读取key对应的value也会修改其最近被使用的时间
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    // [默认的]只要状态超时就访问不到了
                    //.setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                    // 即使数据超时,只要没被清除,就可以被访问到
                    .build();
            //将ttl配置关联到状态描述器上
            ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("count-state", Integer.class);
            stateDescriptor.enableTimeToLive(ttlConfig);
            valueState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
            Integer historyCount = valueState.value();
            if(historyCount == null){
                historyCount = 0;
            }
            int sum = historyCount +value.f1;
            valueState.update(sum);
            value.f1 = sum;
            return value;
        }
    }
}
