package cn.doitedu.flink.day10;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day10
 * @className: D06_KafkaToKafkaDemo1
 * @author: Cypress_Xiao
 * @description: Flink从kafka中读取数据,然后再将结果写回到Kafka中
 * @date: 2022/9/6 15:36
 * @version: 1.0
 */

public class D06_KafkaToKafkaDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(15000, CheckpointingMode.EXACTLY_ONCE);
        DataStreamSource<String> socketLines = env.socketTextStream("hadoop001", 88888);
        SingleOutputStreamOperator<String> errorLines = socketLines.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                if (value.startsWith("error")) {
                    throw new RuntimeException("出现错误数据,抛出异常");
                }
                return value;
            }
        });

        String topic = "doit32";
        List<String> topics = Arrays.asList(topic.split(","));
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop001:9092,hadoop002:9092");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");



        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(topics,new SimpleStringSchema(),props);

        //checkpoint成功,不将偏移量写入到kafka特殊的topic中
        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);
        //调用addSource,传入kafkaConsumer
        DataStreamSource<String> lines = env.addSource(kafkaConsumer);
        DataStream<String> union = lines.union(errorLines);
        SingleOutputStreamOperator<String> filter = union.filter(line -> !line.startsWith("error"));

        //写回到kafka中
        String outTopic ="log-out";
        //事务的超时时间改为10min,broker默认值是15min
        props.setProperty("transaction.timeout.ms", "600000");

        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>(
                outTopic,
                new MyKafkaSerializationSchema(outTopic,"UTF-8"),
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );

        filter.addSink(kafkaProducer);
        env.execute();

    }

    private static class MyKafkaSerializationSchema implements KafkaSerializationSchema<String>{
        private String topic;
        private String charset;

        public MyKafkaSerializationSchema(String outTopic, String charset) {
            this.charset = charset;
            this.topic = outTopic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
            return new ProducerRecord<>(topic,element.getBytes(Charset.forName(charset)));
        }
    }
}
