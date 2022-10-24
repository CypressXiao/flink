package cn.doitedu.flink.day01;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day01
 * @className: D2_SocketSourceDemo
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/8/24 15:25
 * @version: 1.0
 */

public class d2_SocketSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        int p = env.getParallelism();
        System.out.println("执行环境的并行度" + p);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);

        int p1 = lines.getParallelism();
        System.out.println("socketSource的并行度" + p1);

        SingleOutputStreamOperator<String> upperStream = lines.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s.toUpperCase();
            }
        });

        int p2 = upperStream.getParallelism();
        System.out.println("upperStream的并行度" + p2);

        DataStreamSink<String> sink = upperStream.print();
        int p3 = sink.getTransformation().getParallelism();
        System.out.println("sink的并行度" + p3);

        env.execute(d2_SocketSourceDemo.class.getSimpleName());

    }
}
