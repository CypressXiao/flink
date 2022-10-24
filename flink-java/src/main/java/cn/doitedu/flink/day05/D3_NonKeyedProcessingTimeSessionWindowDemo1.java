package cn.doitedu.flink.day05;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day05
 * @className: D3_NonKeyedProcessingTimeSessionWindowDemo1
 * @author: Cypress_Xiao
 * @description: ProcessingTimeSessionWindow触发机制是:当前系统时间 - 进入到该窗口的最后一条数据的时间 > 指定的时间间隔
 * @date: 2022/8/30 11:12
 * @version: 1.0
 */

public class D3_NonKeyedProcessingTimeSessionWindowDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        SingleOutputStreamOperator<Integer> map = lines.map(Integer::parseInt);
        AllWindowedStream<Integer, TimeWindow> windowedStream = map.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));
        SingleOutputStreamOperator<Integer> res = windowedStream.sum(0);
        res.print();
        env.execute();


    }
}
