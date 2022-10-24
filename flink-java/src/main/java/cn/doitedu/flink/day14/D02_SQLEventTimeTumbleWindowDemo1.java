package cn.doitedu.flink.day14;

import cn.doitedu.flink.day13.FlinkSQLUtils;

import java.io.IOException;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day14
 * @className: D02_SQLEventTimeTumbleWindowDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/9/12 9:38
 * @version: 1.0
 */

public class D02_SQLEventTimeTumbleWindowDemo1 {
    public static void main(String[] args) throws IOException {
        FlinkSQLUtils.execute("src\\main\\java\\cn\\doitedu\\flink\\sqls\\day14\\EventTimeTumbleWindow.sql");
    }
}
