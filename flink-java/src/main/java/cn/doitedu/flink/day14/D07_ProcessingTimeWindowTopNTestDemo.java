package cn.doitedu.flink.day14;

import cn.doitedu.flink.day13.FlinkSQLUtils;

import java.io.IOException;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day14
 * @className: D07_WindowTopNDemo1
 * @author: Cypress_Xiao
 * @description: 目前windowTopN不支持processingTime
 * @date: 2022/9/12 11:46
 * @version: 1.0
 */

public class D07_ProcessingTimeWindowTopNTestDemo {
    public static void main(String[] args) throws IOException {
        FlinkSQLUtils.execute("src\\main\\java\\cn\\doitedu\\flink\\sqls\\day14\\ProcessingTimeWindowTopNTest(不支持).sql");
    }
}
