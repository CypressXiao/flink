package cn.doitedu.flink.day05;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day05
 * @className: D1_ProcessingTimeSlidingWindowAllDemo1
 * @author: Cypress_Xiao
 * @description: 按照ProcessingTime划分滑动窗口,没有keyBy,直接划分
 * @date: 2022/8/30 10:04
 * @version: 1.0
 */

public class D1_NonKeyedProcessingTimeSlidingWindowDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        SingleOutputStreamOperator<Integer> map = lines.map(Integer::parseInt);
        AllWindowedStream<Integer, TimeWindow> windowedStream = map.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(20), Time.seconds(10)));
        SingleOutputStreamOperator<Integer> res = windowedStream.sum(0);
        res.print();
        env.execute();

    }
}
