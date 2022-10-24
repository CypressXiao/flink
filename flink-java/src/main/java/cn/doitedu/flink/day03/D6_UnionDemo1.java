package cn.doitedu.flink.day03;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day03
 * @className: D6_UnionDemo1
 * @author: Cypress_Xiao
 * @description: 将两个或两个以上相同类型的数据流合并到一起
 * @date: 2022/8/27 14:31
 * @version: 1.0
 */

public class D6_UnionDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(4);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        DataStreamSource<String> lines2 = env.socketTextStream("hadoop001", 8899);
        SingleOutputStreamOperator<String> m1 = lines.map(String::toUpperCase).setParallelism(2);
        SingleOutputStreamOperator<String> m2 = lines2.map(String::toLowerCase).setParallelism(4);

        DataStream<String> union = m1.union(m2);

        union.print();
        env.execute();
    }
}
