package cn.doitedu.flink.day02;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day02
 * @className: D12_FlatMapDemo2
 * @author: Cypress_Xiao
 * @description: 自定义flatmap
 * @date: 2022/8/25 15:39
 * @version: 1.0
 */

public class D12_FlatMapDemo2 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = lines.transform("MyFlatMap",
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
        }), new MyFlatMap());
        res.print();
        env.execute();
    }

    private static class MyFlatMap extends AbstractStreamOperator<Tuple2<String, Integer>> implements OneInputStreamOperator<String, Tuple2<String, Integer>> {
        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            String[] words = element.getValue().split("\\s+");
            for (String word : words) {
                output.collect(element.replace(Tuple2.of(word, 1)));
            }
        }
    }
}
