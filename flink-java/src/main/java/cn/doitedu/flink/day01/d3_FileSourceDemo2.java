package cn.doitedu.flink.day01;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day01
 * @className: d3_FileSourceDemo2
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/8/24 16:08
 * @version: 1.0
 */

public class d3_FileSourceDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String filePath = "D:\\AllContent\\flink\\flink-java\\data\\words.txt";
        TextInputFormat format = new TextInputFormat(new Path(filePath));

        DataStreamSource<String> lines = env.readFile(format, filePath, FileProcessingMode.PROCESS_CONTINUOUSLY,1000);
        lines.print();
        env.execute();


    }
}
