package cn.doitedu.flink.day04;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.Collection;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day04
 * @className: D10_ProcessingTimeWindowDemo1
 * @author: Cypress_Xiao
 * @description: 按当前系统处理数据的时间划分窗口,没有keyBy直接按照processTime划分窗口
 * @date: 2022/8/28 17:33
 * @version: 1.0
 */

public class D10_ProcessingTimeWindowAllDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);

        SingleOutputStreamOperator<Integer> numStream = lines.map(Integer::parseInt);
        //不keyBy的window都是调的windowAll方法
        //TumblingProcessingTimeWindows滚动窗口,每隔10秒钟划分一个窗口
        AllWindowedStream<Integer, TimeWindow> windowedStream = numStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<Integer> res = windowedStream.sum(0);
        res.print();
        env.execute();


    }
}
