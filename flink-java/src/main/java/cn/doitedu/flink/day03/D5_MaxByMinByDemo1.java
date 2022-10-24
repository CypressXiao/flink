package cn.doitedu.flink.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day03
 * @className: D5_MaxByMinByDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/8/27 11:37
 * @version: 1.0
 */

public class D5_MaxByMinByDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(4);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);

        //四川省,成都市,10000
        //福建省,厦门市,11000
        //福建省,福州市,12000
        SingleOutputStreamOperator<Tuple3<String,String, Integer>> tpStream =
                lines.map(new MapFunction<String, Tuple3<String,String, Integer>>() {
            @Override
            public Tuple3<String,String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0],fields[1], Integer.parseInt(fields[2]));
            }
        });
        KeyedStream<Tuple3<String,String, Integer>, String> keyedStream = tpStream.keyBy(tp -> tp.f0);
        //SingleOutputStreamOperator<Tuple3<String,String, Integer>> max = keyedStream.max(2);
        SingleOutputStreamOperator<Tuple3<String,String, Integer>> max = keyedStream.maxBy(2);
        max.print();
        env.execute();


    }
}
