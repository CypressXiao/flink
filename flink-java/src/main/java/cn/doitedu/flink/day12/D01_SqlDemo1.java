package cn.doitedu.flink.day12;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day12
 * @className: D01_SqlDemo1
 * @author: Cypress_Xiao
 * @description: 创建视图并查询
 * @date: 2022/9/9 9:14
 * @version: 1.0
 */

public class D01_SqlDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //创建增强的env
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(15000);

        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, String>> tpStream = lines.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], fields[2]);
            }
        });
        //注册视图
        tEnv.createTemporaryView("v_aid_log",tpStream);

        //查询
        Table table = tEnv.sqlQuery("select * from v_aid_log");

        //打印结果需要转化为DataStream
        //该转流方式不支持聚合操作,聚合操作不是单纯的将数据追加,而是会更新原来的数据
        /*DataStream<Tuple3<String, String, String>> res = tEnv.toAppendStream(table, TypeInformation.of(new TypeHint<Tuple3<String, String, String>>() {
        }));*/
        //可以删减的流
        DataStream<Tuple2<Boolean, Tuple3<String, String, String>>> res = tEnv.toRetractStream(table, TypeInformation.of(new TypeHint<Tuple3<String, String, String>>() {
        }));


        res.print();

        env.execute();
    }
}
