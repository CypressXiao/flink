package cn.doitedu.flink.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day02
 * @className: D4_MyPrintSinkDemo1
 * @author: Cypress_Xiao
 * @description: 自定义一个打印的sink
 * @date: 2022/8/25 10:39
 * @version: 1.0
 */

public class D4_MyPrintSinkDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        SingleOutputStreamOperator<String> upperStream = lines.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value.toUpperCase();
            }
        });

        //自定义sink方法
        upperStream.addSink(new MyPrintSink());

        env.execute();
    }

    private static class MyPrintSink implements SinkFunction<String>{

        //每来一条数据调用一次invoke方法
        @Override
        public void invoke(String value, Context context) throws Exception {
            System.out.println(value);
        }
    }
}
