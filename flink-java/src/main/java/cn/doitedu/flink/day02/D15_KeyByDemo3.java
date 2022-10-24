package cn.doitedu.flink.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;




/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day02
 * @className: D15_KeyByDemo3
 * @author: Cypress_Xiao
 * @description: 将多个字段联合起来进行分组
 * @date: 2022/8/25 17:02
 * @version: 1.0
 */

/**
 * 四川省,成都市,1000
 * 四川省,绵阳市,2000
 * 广东省,广州市,3000
 * 广东省,深圳市,4000
 */

public class D15_KeyByDemo3 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        SingleOutputStreamOperator<Tuple3<String, String, Double>> tpStream = lines.flatMap(new FlatMapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public void flatMap(String line, Collector<Tuple3<String, String, Double>> out) throws Exception {
                String[] split = line.split(",");
                out.collect(Tuple3.of(split[0], split[1], Double.parseDouble(split[2])));
            }
        });

        /*KeyedStream<Tuple3<String, String, Double>, Tuple2<String, String>> keyedStream = tpStream.keyBy(new KeySelector<Tuple3<String, String, Double>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple3<String, String, Double> value) throws Exception {
                return Tuple2.of(value.f0, value.f1);
            }
        });*/

        /*KeyedStream<Tuple3<String, String, Double>, Tuple> keyedStream = tpStream.keyBy(0,1);*/

        /*KeyedStream<Tuple3<String, String, Double>, Tuple> keyedStream = tpStream.keyBy("f0", "f1");*/

        KeyedStream<Tuple3<String, String, Double>, Tuple2<String, String>> keyedStream = tpStream.keyBy(value -> Tuple2.of(value.f0, value.f1), TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
        }));

        keyedStream.print();
        env.execute();
    }
}
