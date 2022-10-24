package cn.doitedu.flink.day02;

import org.apache.flink.api.common.typeinfo.TypeInformation;
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
 * @className: D10_FilterDemo2
 * @author: Cypress_Xiao
 * @description: filter底层实现
 * @date: 2022/8/25 15:05
 * @version: 1.0
 */

public class D10_FilterDemo2 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        SingleOutputStreamOperator<String> res = lines.transform("MyFilter", TypeInformation.of(String.class), new MyFilter());
        res.print();
        env.execute();
    }

    //AbstractStreamOperator的泛型是输出结果的数据类型
    private static class MyFilter extends AbstractStreamOperator<String> implements OneInputStreamOperator<String, String> {
        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            if (element.getValue().startsWith("h")) {
                output.collect(element);
            }
        }
    }
}
