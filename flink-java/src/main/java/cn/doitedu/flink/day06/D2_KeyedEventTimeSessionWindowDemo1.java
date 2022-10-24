package cn.doitedu.flink.day06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day06
 * @className: D2_KeyedEventTimeSessionWindowDemo1
 * @author: Cypress_Xiao
 * @description: 先keyBy然后按照eventTime划分窗口,
 * KeyedEventTimeSessionWindow触发条件:窗口的watermark - 进入到每个组内最后一条数据eventTime > 指定时间间隔
 * @date: 2022/8/31 10:49
 * @version: 1.0
 */

public class D2_KeyedEventTimeSessionWindowDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);

        SingleOutputStreamOperator<String> watermarks = lines.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String element) {
                String[] fields = element.split(",");
                return Long.parseLong(fields[0]);
            }
        });

        watermarks.map((MapFunction<String, Tuple2<String, Integer>>) value -> {
            String[] fields = value.split(",");
            return Tuple2.of(fields[1], Integer.parseInt(fields[2]));
        }).returns(new TypeHint<Tuple2<String, Integer>>() {
        }).keyBy(t -> t.f0)
                .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
                .sum(1)
                .print();
        env.execute();
    }
}
