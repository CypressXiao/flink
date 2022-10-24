
package cn.doitedu.flink.day06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.tools.nsc.transform.patmat.ScalaLogic;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day06
 * @className: D4_RestartStrategyDemo1
 * @author: Cypress_Xiao
 * @description: 演示flink的重启策略, 是能够保证容错的重要机制
 * @date: 2022/8/31 14:36
 * @version: 1.0
 */

public class D4_RestartStrategyDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //fixedDelayRestart:可以重启固定次数的策略,每次重启延迟指定的时间
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000));

        //第一个参数代表在第二个参数的时间窗口内,最多出现错误的次数;第三个参数代表每次重启延迟的时间;
        //超过了指定的时间范围,会重新计数(错误次数会恢复为0)
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.seconds(30),Time.seconds(3)));

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
