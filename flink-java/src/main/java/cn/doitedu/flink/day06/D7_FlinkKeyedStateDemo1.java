package cn.doitedu.flink.day06;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day06
 * @className: D7_FlinkKeyedStateDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/8/31 16:59
 * @version: 1.0
 */

public class D7_FlinkKeyedStateDemo1 {
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

        SingleOutputStreamOperator<Tuple2<String, Integer>> res = keyed.map(new MyWcValueState());

        res.print();

        env.execute();


    }

    public static class MyWcValueState extends RichMapFunction<Tuple2<String, Integer>,Tuple2<String, Integer>>{
        private ValueState<Integer> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            /*//定义一个状态描述器(描述状态的的名称,类型)
            ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("count-state", Integer.class);
            //初始化或恢复状态
             valueState = getRuntimeContext().getState(stateDescriptor);*/
            ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("count-state", Integer.class);
            valueState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
            /*String key = value.f0;
            //根据当前的key取value
            Integer historyCount = valueState.value();
            if(historyCount == null){
                historyCount = 0;
            }
            int sum = historyCount + value.f1;
            //更新
            valueState.update(sum);
            return Tuple2.of(key,sum);*/
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
