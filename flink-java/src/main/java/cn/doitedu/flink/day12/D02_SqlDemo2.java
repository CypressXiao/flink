package cn.doitedu.flink.day12;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day12
 * @className: D02_SqlDemo2
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/9/9 9:30
 * @version: 1.0
 */

public class D02_SqlDemo2 {
    public static void main(String[] args) throws Exception {
        //StreamExecutionEnvironment只能编写DataStream的API
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //将原来的StreamExecutionEnvironment进行包装，增强后，就可以写SQL
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //spark,3
        //hive,2
        //hbase,6
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String line) throws Exception {
                String[] fields = line.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });

        //根据DataStream窗口视图
        tEnv.createTemporaryView("v_wc", tpStream, "word,counts");
        Table table = tEnv.sqlQuery("select word, sum(counts) counts from v_wc group by word");
        //DataStream<Tuple2<Boolean, Row>> res = tEnv.toRetractStream(table, Row.class);
        //toAppendStream和toDataStream只能在没有聚合类的table使用,toDataStream是新的api
        //DataStream<Row> res = tEnv.toDataStream(table,Row.class);
        //toChangelogStream是toRetractStream新的api
        DataStream<Row> res = tEnv.toChangelogStream(table);
        res.print();
        env.execute();
    }
}
