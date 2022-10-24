package cn.doitedu.flink.day03;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day03
 * @className: D7_ConnectDemo1
 * @author: Cypress_Xiao
 * @description: 数据流是相互独立的
 * @date: 2022/8/27 14:53
 * @version: 1.0
 */

public class D7_ConnectDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<String> line1 = env.socketTextStream("hadoop001", 8888);
        SingleOutputStreamOperator<String> m1 = line1.map(String::toUpperCase);
        DataStreamSource<String> line2 = env.socketTextStream("hadoop001", 8899);
        SingleOutputStreamOperator<Integer> m2 = line2.map(Integer::parseInt);

        //保留了原来两个数据流各自额数据类型,只能调用部分transformation算子
        ConnectedStreams<String, Integer> connect = m1.connect(m2);
        SingleOutputStreamOperator<String> map = connect.map(new CoMapFunction<String, Integer, String>() {
            @Override
            public String map1(String value) throws Exception {
                return value.toLowerCase();
            }

            @Override
            public String map2(Integer value) throws Exception {
                return value.toString();
            }
        });

        map.print();
        env.execute();
    }
}
