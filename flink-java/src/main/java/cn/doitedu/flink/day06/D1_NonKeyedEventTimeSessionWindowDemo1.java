package cn.doitedu.flink.day06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import scala.Tuple2;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day06
 * @className: D1_NonKeyedEventTimeSessionWindowDemo1
 * @author: Cypress_Xiao
 * @description: 按照eventTime划分的会话窗口, 不keyBy直接划分
 * @date: 2022/8/31 10:38
 * @version: 1.0
 */

public class D1_NonKeyedEventTimeSessionWindowDemo1 {
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

        watermarks.map((MapFunction<String, Integer>) value -> {
            String[] fields = value.split(",");
            return Integer.parseInt(fields[1]);
        }).windowAll(EventTimeSessionWindows.withGap(Time.seconds(5)))
                .sum(0)
                .print();
        env.execute();

    }
}
