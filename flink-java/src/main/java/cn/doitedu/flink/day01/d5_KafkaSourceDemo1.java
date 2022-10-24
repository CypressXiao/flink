package cn.doitedu.flink.day01;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day01
 * @className: d5_KafkaSourceDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/8/24 17:25
 * @version: 1.0
 */

public class d5_KafkaSourceDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //创建配置文件对象,并进行配置
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop001:9092,hadoop002:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "g001");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        String topic = "doit32";
        List<String> topics = Arrays.asList(topic.split(","));
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer(topics, new SimpleStringSchema(), props);

        //使用addSource创建DataStream
        DataStreamSource<String> lines = env.addSource(kafkaConsumer);
        System.out.println(lines.getParallelism());

        lines.print();

        env.execute();


    }
}
