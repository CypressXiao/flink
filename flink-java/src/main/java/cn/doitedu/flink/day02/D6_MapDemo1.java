package cn.doitedu.flink.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day02
 * @className: D6_MapDemo1
 * @author: Cypress_Xiao
 * @description: 使用Lambda表达式时,如果返会的DataStream中的数据类型还有泛型,会存在泛型擦除,需使用return手动添加
 * @date: 2022/8/25 11:14
 * @version: 1.0
 */

public class D6_MapDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        /*SingleOutputStreamOperator<Tuple2<String, String>> tp = lines.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String line) throws Exception {
                String[] ss = line.split(",");
                return Tuple2.of(ss[0], ss[1]);
            }
        });*/

        SingleOutputStreamOperator<Tuple2<String, String>> tp = lines.map( line -> {
            String[] ss = line.split(",");
            return Tuple2.of(ss[0], ss[1]);
        }).returns(Types.TUPLE(Types.STRING,Types.STRING));

        tp.print();

        env.execute();

    }
}
