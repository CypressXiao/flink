package cn.doitedu.flink.day09;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day09
 * @className: D03_KeyedProcessFunctionDemo1
 * @author: Cypress_Xiao
 * @description: keyedProcessFunction中使用定时器,可以将数据增量聚合或攒起来,自定义触发规则(Window底层使用的就是Timer)
 * @date: 2022/9/5 11:38
 * @version: 1.0
 */

public class D03_KeyedProcessFunctionDemo1 {
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

        SingleOutputStreamOperator<Tuple2<String, Integer>> res = keyed.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                Integer historyCount = valueState.value();
                if (historyCount == null) {
                    historyCount = 0;
                }
                int sum = historyCount + value.f1;
                valueState.update(sum);
                value.f1 = sum;
                out.collect(value);
            }

            private ValueState<Integer> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("count-state", Integer.class);
                valueState = getRuntimeContext().getState(stateDescriptor);
            }
        });


        res.print();

        env.execute();

    }
}

