package cn.doitedu.flink.day12;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day12
 * @className: PracticeDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/9/9 10:06
 * @version: 1.0
 */

public class PracticeDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        SingleOutputStreamOperator<Tuple3<String, String, Double>> tpStream = lines.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String value) throws Exception {
                if (value.startsWith(",")) {
                    throw new RuntimeException("输入不合法!");
                }
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], Double.parseDouble(fields[2]));
            }
        });

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.createTemporaryView("count_tb", tpStream, $("gid"), $("lid"), $("money"), $("p_time").proctime());
        //划窗口进行增量聚合统计时,需要两个group by,第一个group by里面获取各个窗口的聚合数据,第二个窗口获取最终的窗口间聚合得到的结果数据
        TableResult tbResult = tEnv.executeSql("select gid,lid,sum(total) as total_money\n" +
                "from\n" +
                "(select gid,lid,sum(money) as total\n" +
                "from\n" +
                "(select gid,lid,money,RAND_INTEGER(4) as num,p_time\n" +
                "from count_tb) as t1\n" +
                "group by gid,lid,num,tumble(p_time,INTERVAL '10' SECONDS)) as t2\n" +
                "group by gid,lid");

        tbResult.print();

        env.execute();
    }
}
