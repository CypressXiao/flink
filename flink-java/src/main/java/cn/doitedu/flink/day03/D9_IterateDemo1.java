package cn.doitedu.flink.day03;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day03
 * @className: D9_IterateDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/8/27 15:29
 * @version: 1.0
 */

public class D9_IterateDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);

        SingleOutputStreamOperator<Long> num = lines.map(Long::parseLong);

        IterativeStream<Long> it = num.iterate();
        SingleOutputStreamOperator<Long> num1 = it.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("middleRes =>"+value);
                return value - 3;
            }
        });

        SingleOutputStreamOperator<Long> feedback = num1.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value > 0;
            }
        });
        //传入继续迭代的条件
        it.closeWith(feedback);

        SingleOutputStreamOperator<Long> finalRes = num1.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value <= 0;
            }
        });

        finalRes.print("finalRes");

        env.execute();
    }
}
