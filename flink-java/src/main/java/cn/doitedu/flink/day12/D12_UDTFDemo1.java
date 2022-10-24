package cn.doitedu.flink.day12;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day12
 * @className: D12_UDTFDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/9/9 19:41
 * @version: 1.0
 */

public class D12_UDTFDemo1 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //hello tom jerry tom
        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);

        //tableEnv.registerDataStream("t_lines", socketTextStream, "line");
        tableEnv.createTemporaryView("t_lines", socketTextStream, $("line"));

        //窗口一个临时函数
        tableEnv.createTemporaryFunction("split", new Split(" "));

        Table table = tableEnv.sqlQuery(
                "SELECT word FROM t_lines, LATERAL TABLE(split(line)) as A(word)");

        //非聚合的Table，将表转成AppendStream
        tableEnv.toAppendStream(table, Row.class).print();

        env.execute();

    }

    public static class Split extends TableFunction<String> {

        private String separator = ",";

        public Split() {}

        public Split(String separator) {
            this.separator = separator;
        }

        public void eval(String line) {
            for (String s : line.split(separator)) {
                collect(s);
            }
        }
    }
}
