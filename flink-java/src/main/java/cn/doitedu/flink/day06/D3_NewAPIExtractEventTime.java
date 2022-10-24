package cn.doitedu.flink.day06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day06
 * @className: D4_NewAPI
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/8/31 11:24
 * @version: 1.0
 */

public class D3_NewAPIExtractEventTime {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        //1000,spark,1
        SingleOutputStreamOperator<String> watermarks = lines.assignTimestampsAndWatermarks(
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                            @Override
                            public long extractTimestamp(String element, long recordTimestamp) {
                                return Long.parseLong(element.split(",")[0]);
                            }
                        })
        );

        watermarks.map((MapFunction<String, Tuple2<String, Integer>>) value -> {
            String[] fields = value.split(",");
            return Tuple2.of(fields[1], Integer.parseInt(fields[2]));
        }).returns(new TypeHint<Tuple2<String, Integer>>() {
        }).keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum(1)
                .print();
        env.execute();


    }
}
