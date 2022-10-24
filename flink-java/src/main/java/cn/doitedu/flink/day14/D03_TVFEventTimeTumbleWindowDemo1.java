package cn.doitedu.flink.day14;

import cn.doitedu.flink.day13.FlinkSQLUtils;

import java.io.IOException;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day14
 * @className: D03_TVFEventTimeTumbleWindowDemo1
 * @author: Cypress_Xiao
 * @description: 使用新的方式对窗口数据进行运算
 * @date: 2022/9/12 10:24
 * @version: 1.0
 */

public class D03_TVFEventTimeTumbleWindowDemo1 {
    public static void main(String[] args) throws IOException {
        FlinkSQLUtils.execute("src\\main\\java\\cn\\doitedu\\flink\\sqls\\day14\\TVFEventTimeTumbleWindow.sql");
    }
}
