package cn.doitedu.flink.day03;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day03
 * @className: D12_ShuffleDemo1
 * @author: Cypress_Xiao
 * @description: 随机分区
 * @date: 2022/8/27 17:51
 * @version: 1.0
 */

public class D12_ShuffleDemo1 {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);

        DataStream<String> mapStream = lines.map(new RichMapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                //获取当前的分区编号
                int index = getRuntimeContext().getIndexOfThisSubtask();
                return value + "->" + index;
            }
        }).setParallelism(2).shuffle();

        mapStream.addSink(new RichSinkFunction<String>() {
            @Override
            public void invoke(String value, SinkFunction.Context context) throws Exception {
                int index1 = getRuntimeContext().getIndexOfThisSubtask();
                System.out.println(value + "->" + index1);
            }
        }).setParallelism(4);


        env.execute();
    }
}
