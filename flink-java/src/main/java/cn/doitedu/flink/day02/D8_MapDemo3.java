package cn.doitedu.flink.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
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
 * @className: D7_MapDemo2
 * @author: Cypress_Xiao
 * @description: map算子底层实现, 并自定义streamMap类
 * @date: 2022/8/25 14:35
 * @version: 1.0
 */

public class D8_MapDemo3 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        /*SingleOutputStreamOperator<Tuple2<String, String>> tp = lines.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String line) throws Exception {
                String[] ss = line.split(",");
                return Tuple2.of(ss[0], ss[1]);
            }
        });*/
        SingleOutputStreamOperator<String> tp = lines.transform("MyMap",
                TypeInformation.of(String.class),
                new MyStreamMap());

        tp.print();

        env.execute();

    }

    private static class MyStreamMap extends AbstractStreamOperator<String> implements OneInputStreamOperator<String, String> {

        /**
         * @param element:
         * @return void
         * @author Cypress_Xiao
         * @description 对每一条数据进行处理, 即来一条数据调一次方法
         * @date 2022/8/25 14:40
         */

        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            //获取StreamRecord中封装的数据
            String value = element.getValue();
            String upper = value.toUpperCase();
            output.collect(element.replace(upper));
        }
    }
}
