package cn.doitedu.flink.day14;

import cn.doitedu.flink.day13.FlinkSQLUtils;

import java.io.IOException;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day14
 * @className: D05_SQLEventTimeCumulateTumbleWindowDemo1
 * @author: Cypress_Xiao
 * @description: 累积窗口,可以累加指定长度的多个窗口数据,比如窗口长度是10min,累加一个小时的数据,下一个小时开始重新累加数据;
 * 要求最大窗口大小必须是窗口步长的整数倍
 * @date: 2022/9/12 10:59
 * @version: 1.0
 */

public class D05_SQLEventTimeCumulateTumbleWindowDemo1 {
    public static void main(String[] args) throws IOException {
        FlinkSQLUtils.execute("src\\main\\java\\cn\\doitedu\\flink\\sqls\\day14\\TVFEventTimeCumulateWindow.sql");
    }
}
