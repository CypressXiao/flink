package cn.doitedu.flink.day09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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

import java.time.Duration;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day09
 * @className: D06_KeyedProcessFunctionDemo2
 * @author: Cypress_Xiao
 * @description: 使用ProcessFunction + Timer + KeyedState 实现类似滚动窗口(eventTime)
 * @date: 2022/9/5 14:47
 * @version: 1.0
 */

public class D06_KeyedProcessFunctionDemo2 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(4);
        env.enableCheckpointing(5000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000));
        //2.调用Source创建抽象数据集
        //使用nc命令开启一个socket服务
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        //生成水位线
        SingleOutputStreamOperator<String> watermarks = lines.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String element, long recordTimestamp) {
                return Long.parseLong(element.split(",")[0]);
            }
        }));

        //1000,spark,1
        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = watermarks.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[1], Integer.parseInt(fields[2]));
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tpStream.keyBy(t -> t.f0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            private transient ValueState<Integer> countState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("count-state", Integer.class);
                countState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                Integer count = countState.value();
                if (count == null) {
                    count = 0;
                }
                count += value.f1;
                countState.update(count);
                long watermark = ctx.timerService().currentWatermark();
                if (watermark >= 0) {
                    long trigger = watermark - watermark % 10000 + 10000;
                    ctx.timerService().registerEventTimeTimer(trigger);
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(Tuple2.of(ctx.getCurrentKey(), countState.value()));
                countState.update(null);
            }
        });

        res.print();
        env.execute();


    }
}
