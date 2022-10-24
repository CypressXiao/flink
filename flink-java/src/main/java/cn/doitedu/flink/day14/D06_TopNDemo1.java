package cn.doitedu.flink.day14;

import cn.doitedu.flink.day13.FlinkSQLUtils;


import java.io.IOException;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day14
 * @className: D06_TopNDemo1
 * @author: Cypress_Xiao
 * @description: 不划分timeWindow,然后分区内求TopN,按照游戏id进行分区,统计相同游戏id,不同分区充值金额的topN
 * @date: 2022/9/12 11:46
 * @version: 1.0
 */

public class D06_TopNDemo1 {
    public static void main(String[] args) throws IOException {

        FlinkSQLUtils.execute("src\\main\\java\\cn\\doitedu\\flink\\sqls\\day14\\TopN.sql");

    }
}
