package cn.doitedu.flink.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day02
 * @className: D5_MyPrintSinkDemo2
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/8/25 10:48
 * @version: 1.0
 */

public class D5_MyPrintSinkDemo2 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        SingleOutputStreamOperator<String> word = lines.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value.toLowerCase();
            }
        });
        word.addSink(new MyRichPrintSink()).setParallelism(4);
        env.execute();
    }

    public static class MyRichPrintSink extends RichSinkFunction<String>{
        private int index;
        @Override
        public void open(Configuration parameters) throws Exception {
            index = getRuntimeContext().getIndexOfThisSubtask() ;
            System.out.println("subtask "+index+" open method invoked!");
        }

        @Override
        public void close() throws Exception {
            System.out.println("subtask "+index+" close method invoked!");
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            System.out.println((index+1)+"> "+value);
        }
    }
}
