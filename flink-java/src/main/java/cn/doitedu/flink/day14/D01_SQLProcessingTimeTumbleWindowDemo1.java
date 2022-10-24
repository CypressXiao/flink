package cn.doitedu.flink.day14;

import cn.doitedu.flink.day13.FlinkSQLUtils;

import java.io.IOException;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day14
 * @className: D01_SQLProcessingTimeTumbleWindowDemo1
 * @author: Cypress_Xiao
 * @description: 使用flink sql实现processingTime的滚动窗口
 * @date: 2022/9/12 9:06
 * @version: 1.0
 */

public class D01_SQLProcessingTimeTumbleWindowDemo1 {
    public static void main(String[] args) throws IOException {
        FlinkSQLUtils.execute("src\\main\\java\\cn\\doitedu\\flink\\sqls\\day14\\ProcessingTimeTumbleWindow.sql");
    }
}
