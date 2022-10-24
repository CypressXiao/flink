package cn.doitedu.flink.day02;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day02
 * @className: D9_FilterDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/8/25 14:52
 * @version: 1.0
 */

public class D9_FilterDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        //SingleOutputStreamOperator<String> res = lines.filter(e -> e.startsWith("h"));
        SingleOutputStreamOperator<String> res = lines.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.startsWith("h");
            }
        });
        res.print();
        env.execute();

    }
}
