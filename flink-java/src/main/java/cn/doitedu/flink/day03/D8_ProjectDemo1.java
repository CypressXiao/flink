package cn.doitedu.flink.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day03
 * @className: D8_ProjectDemo1
 * @author: Cypress_Xiao
 * @description: 可以对ds中类型为tuple的数据进行映射
 * @date: 2022/8/27 15:07
 * @version: 1.0
 */

public class D8_ProjectDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
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
        SingleOutputStreamOperator<Tuple> project = tpStream.project(2, 1);
        project.print();
        env.execute();

    }
}
