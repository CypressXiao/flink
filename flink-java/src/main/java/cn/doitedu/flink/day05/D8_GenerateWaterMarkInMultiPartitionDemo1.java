package cn.doitedu.flink.day05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
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
 * @className: D8_GenerateWaterMarkInMultiPartitionDemo1
 * @author: Cypress_Xiao
 * @description: 生成waterMark对应的dataStream有多个分区,每个分区发送的WaterMark = 当前分区中最大的EventTime - 延迟时间,
                 整个窗口的waterMark为所有分区里的最小waterMark
 * @date: 2022/8/30 16:18
 * @version: 1.0
 */

public class D8_GenerateWaterMarkInMultiPartitionDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        env.setParallelism(4);
        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> tp = lines.map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
            @Override
            public Tuple3<Long, String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(Long.parseLong(fields[0]), fields[1], Integer.parseInt(fields[2]));
            }
        });

        //1661832000000,spark,1
        //1661832001000,hive,1
        //1661832002000,flink,2
        //1661832003000,spark,4
        //1661832005000,hive,6

        //提取数据时间,并转成timestamp类型
        //提取完eventTime的linesWithWaterMark里面的数据依然是原来的格式,只不过会根据提取的eventTime生成waterMark
        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> linesWithWaterMark = tp.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Long, String, Integer>>(Time.seconds(0)) {

            @Override
            public long extractTimestamp(Tuple3<Long, String, Integer> element) {
                return element.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = linesWithWaterMark.map(new MapFunction<Tuple3<Long, String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple3<Long, String, Integer> value) throws Exception {
                return Tuple2.of(value.f1,value.f2);
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tpStream.keyBy(t -> t.f0);

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        SingleOutputStreamOperator<Tuple2<String, Integer>> res = windowedStream.sum(1);

        res.print();

        env.execute();
    }
}
