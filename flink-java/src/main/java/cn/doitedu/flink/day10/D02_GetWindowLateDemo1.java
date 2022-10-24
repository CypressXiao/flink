package cn.doitedu.flink.day10;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day10
 * @className: D11_GetWindowLateDemo1
 * @author: Cypress_Xiao
 * @description: 划分eventTime类型的滚动窗口, 使用测流输出的方式获取迟到的数据(processTime不存在数据迟到的问题, 因此侧流输出主要针对eventTime
 * @date: 2022/9/6 11:36
 * @version: 1.0
 */

public class D02_GetWindowLateDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        SingleOutputStreamOperator<String> watermarks = lines.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String element, long recordTimestamp) {
                return Long.parseLong(element.split(",")[0]);
            }
        }));

        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = watermarks.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tpStream.keyBy(t -> t.f0);

        //侧流标签需要手动创建
        OutputTag<Tuple2<String, Integer>> lateTag = new OutputTag<Tuple2<String, Integer>>("lateTag");

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(0)))
                .sideOutputLateData(lateTag);//将迟到的数据打上指定的标签

        //处理的是未打标签的数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = windowedStream.sum(1);

        //处理打标签的数据
        DataStream<Tuple2<String, Integer>> lateStream = res.getSideOutput(lateTag);

        res.print();
        lateStream.print("late");
        env.execute();

    }
}
