package cn.doitedu.flink.day06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day06
 * @className: D4_RestartStrategyDemo2
 * @author: Cypress_Xiao
 * @description: 开启checkpointing(检查点)
 * @date: 2022/8/31 15:11
 * @version: 1.0
 */

public class D5_RestartStrategyDemo2 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //开启checkpointing,那么job就会默认fixedDelayRestart策略,重启次数是Integer的最大值
        env.enableCheckpointing(5000);

        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        //默认情况下,如果没有开启checkpoint,flink程序就没有设置重启策略
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordStream = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                if (value.startsWith("error")) {
                    throw new RuntimeException("有错误数据出现,抛出异常!");
                }
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });
        wordStream.keyBy(t->t.f0).sum(1).print();
        env.execute();
    }
}
