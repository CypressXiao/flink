package cn.doitedu.flink.day07;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day07
 * @className: PracticeDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/9/2 9:25
 * @version: 1.0
 */

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.List;

/**
 * 统计广告的曝光、点击的人数和次数
 * <p>
 * u001,view,a001
 * u002,view,a001
 * u002,click,a001
 * u001,view,a001
 * u002,view,a002
 * u003,view,a002
 * u003,view,a002
 * u003,click,a002
 * u003,click,a002
 */
public class PracticeDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(4);
        env.enableCheckpointing(5000);

        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        SingleOutputStreamOperator<Tuple4<String, String, Integer, Integer>> res = lines.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                if(value.startsWith("error")){
                    throw new RuntimeException("错了重启!");
                }
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], fields[2]);
            }
        }).keyBy(new KeySelector<Tuple3<String, String, String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple3<String, String, String> value) throws Exception {
                return Tuple2.of(value.f1, value.f2);
            }
        })
                .process(new MyState());

        res.print();

        env.execute();

    }

    public static class MyState
            extends KeyedProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, Tuple4<String, String, Integer, Integer>> {

        private ValueState<Integer> countState;
        private ValueState<HashSet<String>> disCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化或者恢复状态
            //定义统计次数的状态
            ValueStateDescriptor<Integer> countStateDesc = new ValueStateDescriptor<Integer>("count-state", Integer.class);
            countState = getRuntimeContext().getState(countStateDesc);
            //定义统计人数的状态
            ValueStateDescriptor<HashSet<String>> disCountStateDesc = new ValueStateDescriptor<HashSet<String>>("discount-state", TypeInformation.of(new TypeHint<HashSet<String>>() {
            }));
            disCountState = getRuntimeContext().getState(disCountStateDesc);
        }

        @Override
        public void processElement(Tuple3<String, String, String> value, Context ctx, Collector<Tuple4<String, String, Integer, Integer>> out) throws Exception {
            Integer count1 = countState.value();
            HashSet<String> countSet = disCountState.value();
            if (countSet == null){
                countSet = new HashSet<>();
            }
            int sum1;
            int sum2;
            if (count1 == null) {
                count1 = 0;
            }
            countSet.add(value.f0);
            sum1 = count1 + 1;
            sum2 = countSet.size();
            countState.update(sum1);
            disCountState.update(countSet);
            out.collect(Tuple4.of(value.f2, value.f1, sum2, sum1));
        }
    }
}
