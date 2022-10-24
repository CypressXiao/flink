package cn.doitedu.flink.day04;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day04
 * @className: D4_OperatorChainDemo1
 * @author: Cypress_Xiao
 * @description: flink的算子链, flink在默认情况下是尽量让算子链连接在一起, 除非遇到并行度不一致或调用了分区算子
 * 好处:减少内存使用;避免线程频繁切换,提高cpu的利用率
 * @date: 2022/8/28 10:16
 * @version: 1.0
 */

public class D4_OperatorChainDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        //强制断开所有的算子链(不建议使用)
        //env.disableOperatorChaining();

        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                for (String s : value.split("\\s+")) {
                    out.collect(s);
                }
            }
        });

        SingleOutputStreamOperator<String> filter = words.filter(word -> !word.startsWith("h"))
                .startNewChain();//从当前算子开启一个新链
                //disableChaining() 将当前算子的前面和后面的链都断开

        SingleOutputStreamOperator<String> map = filter.map(String::toUpperCase);

        KeyedStream<String, String> keyedStream = map.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        });
        keyedStream.print();
        env.execute();


    }
}
