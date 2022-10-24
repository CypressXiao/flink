package cn.doitedu.flink.day13;

import java.io.IOException;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day13
 * @className: D05_KafkaToMysqlDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/9/11 11:31
 * @version: 1.0
 */

public class D05_KafkaToMysqlDemo1 {
    public static void main(String[] args) throws IOException {
        //FlinkSQLUtils.execute("src\\main\\java\\cn\\doitedu\\flink\\day13\\sqls\\gameMoneyCount.sql");
        //FlinkSQLUtils.execute("src\\main\\java\\cn\\doitedu\\flink\\day13\\sqls\\countDistinct.sql");
        FlinkSQLUtils.execute("src\\main\\java\\cn\\doitedu\\flink\\day13\\sqls\\gameWindowCount.sql");
    }
}
