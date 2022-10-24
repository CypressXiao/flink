package cn.doitedu.flink.day04;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day04
 * @className: D2_CustomPartitionDemo1
 * @author: Cypress_Xiao
 * @description: 自定义分区
 * @date: 2022/8/28 9:32
 * @version: 1.0
 */

public class D2_CustomPartitionDemo1 {
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
                return value + " -> " + index;
            }
        }).setParallelism(2).partitionCustom(new Partitioner<String>() {
            int index = 0;

            @Override
            public int partition(String key, int numPartitions) {
                if ("spark".equals(key)) {
                    index = 1;
                } else if ("hive".equals(key)) {
                    index = 2;
                } else if ("flink".equals(key)) {
                    index = 3;
                } else {
                    index = 0;
                }
                return index;
            }
        }, new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value.split("\\s+")[0];
            }
        });

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
