package cn.doitedu.flink.day05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day05
 * @className: D5_KeyedEventTimeTumblingWindowWithLateTimeDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/8/30 16:06
 * @version: 1.0
 */

public class D7_KeyedEventTimeTumblingWindowWithLateTimeDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);

        //1661832000000,spark,1
        //1661832001000,hive,1
        //1661832002000,flink,2
        //1661832003000,spark,4
        //1661832005000,hive,6

        //提取数据时间,并转成timestamp类型
        //提取完eventTime的linesWithWaterMark里面的数据依然是原来的格式,只不过会根据提取的eventTime生成waterMark
        //waterMark是eventTime类型窗口触发的特殊信号,waterMark = eventTime里的最大时间 - 延迟时间
        //eventTime触发的条件是:waterMark >= 窗口的结束时间
        SingleOutputStreamOperator<String> linesWithWaterMark = lines.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String element) {
                String[] fields = element.split(",");
                return Long.parseLong(fields[0]);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = linesWithWaterMark.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[1], Integer.parseInt(fields[2]));
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tpStream.keyBy(t -> t.f0);

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        SingleOutputStreamOperator<Tuple2<String, Integer>> res = windowedStream.sum(1);

        res.print();

        env.execute();





    }
}
