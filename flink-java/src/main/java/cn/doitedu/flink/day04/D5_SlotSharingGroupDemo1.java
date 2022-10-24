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
 * @className: D5_SlotSharingGroupDemo1
 * @author: Cypress_Xiao
 * @description: 设置flink共享资源槽, 默认情况下subtask对应的名为default, subtask进入槽内对应的槽的名称就与subtask
 * 默认标签名字保持一致,以后只有同一个job,不同的task的subtask,并且资源槽名称相同的才可以进入
 * @date: 2022/8/28 14:35
 * @version: 1.0
 */

public class D5_SlotSharingGroupDemo1 {
    public static void main(String[] args) throws Exception {
        /*Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);*/
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream(args[0], Integer.parseInt(args[1]));
        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                for (String s : value.split("\\s+")) {
                    out.collect(s);
                }
            }
        });

        SingleOutputStreamOperator<String> filter = words.filter(word -> !word.startsWith("h"))
                .disableChaining()//将当前算子的前面和后面的链都断开
                .slotSharingGroup("doit"); //该算子有复杂逻辑,将其打上名称

        SingleOutputStreamOperator<String> map = filter.map(String::toUpperCase)
                .slotSharingGroup("default");//将后续算子的名称重新改回去

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
