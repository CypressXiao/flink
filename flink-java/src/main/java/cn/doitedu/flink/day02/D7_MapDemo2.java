package cn.doitedu.flink.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamMap;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day02
 * @className: D7_MapDemo2
 * @author: Cypress_Xiao
 * @description: map算子底层实现
 * @date: 2022/8/25 14:50
 * @version: 1.0
 */

public class D7_MapDemo2 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        MapFunction<String, String> mapFunction = new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value.toUpperCase();
            }
        };
        SingleOutputStreamOperator<String> res = lines.transform("MyMap", TypeInformation.of(String.class), new StreamMap<>(mapFunction));
        res.print();
        env.execute();

    }
}
