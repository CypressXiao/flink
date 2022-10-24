package cn.doitedu.flink.day13;

import java.io.IOException;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day13
 * @className: D06_QueryDimensionFromMysqlDemo1
 * @author: Cypress_Xiao
 * @description: 查询mysql维表关联维度数据
 * @date: 2022/9/11 16:01
 * @version: 1.0
 */

public class D06_QueryDimensionFromMysqlDemo1 {
    public static void main(String[] args) throws IOException {
        FlinkSQLUtils.execute("src\\main\\java\\cn\\doitedu\\flink\\day13\\sqls\\MysqlDimension.sql");

    }
}
