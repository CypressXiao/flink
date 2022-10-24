package cn.doitedu.flink.day01;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day01
 * @className: d3_FileSourceDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/8/24 16:00
 * @version: 1.0
 */

public class d3_FileSourceDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.readTextFile("data\\words.txt");
        lines.print();
        env.execute();
    }
}
