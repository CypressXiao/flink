package cn.doitedu.flink.day12;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day12
 * @className: D09_UDFDemo1
 * @author: Cypress_Xiao
 * @description: FlinkSQL支持自定义函数
 * UDF:输入一行返回一行
 * UDAF:输入多行返回一行
 * UDTF:输入一行返回多行
 * @date: 2022/9/9 17:05
 * @version: 1.0
 */

public class D09_UDFDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //创建增强的env
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //浙江省,杭州市,1000
        //山东省,烟台市,2000
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, String>> tpStream = lines.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], fields[2]);
            }
        });
        //注册视图
        tEnv.createTemporaryView("v_order_log", tpStream);
        //先注册函数
        tEnv.createTemporaryFunction("myconcat", MyConcat.class);

        TableResult tableResult = tEnv.executeSql("select myconcat(f0,f1) as location,cast(f2 as double) as money from v_order_log");
        tableResult.print();
        env.execute();

    }

    public static class MyConcat extends ScalarFunction {
        public String eval(String... fields) {
            StringBuilder res = new StringBuilder();
            for (String s : fields) {
                res.append(s);
            }
            return res.toString();
        }
    }
}
