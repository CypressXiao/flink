package cn.doitedu.flink.day12;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day12
 * @className: D11_UDAFDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/9/9 19:38
 * @version: 1.0
 */

public class D11_UDAFDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //计算平均年龄
        //tom,18,male
        //jerry,16,female
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple3<String, Integer, String>> tpStream = lines.map(new MapFunction<String, Tuple3<String, Integer, String>>() {
            @Override
            public Tuple3<String, Integer, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], Integer.parseInt(fields[1]), fields[2]);
            }
        });

        tEnv.createTemporaryView("v_users", tpStream, $("name"), $("age"), $("gender"));

        //先注册一个自定义函数
        tEnv.createTemporaryFunction("my_avg", MyAvgFunction.class);

        TableResult tableResult = tEnv.executeSql("select gender, my_avg(age) avg_age from v_users group by gender");

        tableResult.print();

        env.execute();

    }


    public static class MyAvgFunction extends AggregateFunction<Double, Tuple2<Double, Integer>> {

        //创建初始值
        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }


        //每来一条数据，就对数据进行运算
        //方法名称必须叫accumulate
        public void accumulate(Tuple2<Double, Integer> acc, Integer age) {
            acc.f0 += age;
            acc.f1 += 1;
        }

        //经过运算，输出的结果
        @Override
        public Double getValue(Tuple2<Double, Integer> tp) {
            return tp.f0 / tp.f1;
        }
    }
}
