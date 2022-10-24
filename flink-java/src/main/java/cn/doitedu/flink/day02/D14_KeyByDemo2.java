package cn.doitedu.flink.day02;


import lombok.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day02
 * @className: D14_KeyByDemo2
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/8/25 16:42
 * @version: 1.0
 */

public class D14_KeyByDemo2 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        //对数据进行压平
        SingleOutputStreamOperator<WordBean> flatMapStream = lines.flatMap(new FlatMapFunction<String, WordBean>() {
            @Override
            public void flatMap(String value, Collector<WordBean> out) throws Exception {
                String[] ss = value.split("\\s+");
                out.collect(new WordBean(ss[0], Integer.parseInt(ss[1])));
            }
        });

        KeyedStream<WordBean, Tuple> keyedStream = flatMapStream.keyBy("word");
        keyedStream.print();
        env.execute();

    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    public static class WordBean {
        private String word;
        private int count;
    }

}


