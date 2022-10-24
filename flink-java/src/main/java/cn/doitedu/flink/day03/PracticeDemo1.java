package cn.doitedu.flink.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;


/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day03
 * @className: PracticeDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/8/27 15:39
 * @version: 1.0
 */

/**
 * 实时产生如下数据，数据的字段含义是：订单id,所在省份,订单金额
 * o001,河北省,3000
 * o002,山东省,2000
 * o003,河北省,4000
 * o004,河北省,1000
 * o005,山东省,3000
 * o006,辽宁省,3000
 * o007,辽宁省,3000
 * <p>
 * 需求1.统计各个生成的成交金额和成交的订单数
 * 需求2.统计全部（国）的成交金额和总订单数量
 */

public class PracticeDemo1 {
    public static void main(String[] args) throws Exception {
        HashMap<String, Tuple3<String, Double, Integer>> map = new HashMap<>();
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        SingleOutputStreamOperator<Tuple3<String, Double, Integer>> map1 = lines.map(new MapFunction<String, Tuple3<String, Double, Integer>>() {
            @Override
            public Tuple3<String, Double, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[1], Double.parseDouble(fields[2]), 1);
            }
        });

        /*SingleOutputStreamOperator<Tuple3<String, Double, Integer>> map2 = lines.map(new MapFunction<String, Tuple3<String, Double, Integer>>() {
            @Override
            public Tuple3<String, Double, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of("全国", Double.parseDouble(fields[2]), 1);
            }
        });

        DataStream<Tuple3<String, Double, Integer>> mp = map1.union(map2);*/

        KeyedStream<Tuple3<String, Double, Integer>, String> keyedStream1 = map1.keyBy(new KeySelector<Tuple3<String, Double, Integer>, String>() {
            @Override
            public String getKey(Tuple3<String, Double, Integer> value) throws Exception {
                return value.f0;
            }
        });


        SingleOutputStreamOperator<Tuple3<String, Double, Integer>> reduce1 = keyedStream1.reduce(new RichReduceFunction<Tuple3<String, Double, Integer>>() {

            @Override
            public Tuple3<String, Double, Integer> reduce(Tuple3<String, Double, Integer> value1, Tuple3<String, Double, Integer> value2) throws Exception {
                value1.f1 = value1.f1 + value2.f1;
                value1.f2 = value1.f2 + value2.f2;
                return value1;
            }
        });

        








        reduce1.print();


        env.execute();


    }
}
