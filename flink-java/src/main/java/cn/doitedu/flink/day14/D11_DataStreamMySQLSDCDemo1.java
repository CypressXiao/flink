package cn.doitedu.flink.day14;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day14
 * @className: D11_DataStreamMySQLSDCDemo1
 * @author: Cypress_Xiao
 * @description: 使用Flink mysql CDC的dataStream API,CDC就是一种特殊的Source,通过读取mysql的binlog(二进制日志文件),
 * 快速高效的同步增量数据
 * 要求:
 * 1.导入mysql的CDC的依赖;
 * 2.修改mysql的配置文件my.conf并重启mysql服务
 * log-bin=mysql-bin #添加这一行就ok
 * binlog-format=ROW #选择row模式
 * server_id=1 #配置mysql replication需要定义，不能与客户端的slaveId重复
 * @date: 2022/9/12 15:39
 * @version: 1.0
 */

public class D11_DataStreamMySQLSDCDemo1 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("localhost") //mysql所在服务的主机名或IP地址
                .port(3306) //MySQL的端口号
                .databaseList("doit32", "my_db1") //指定DataBase的名字
                .tableList("doit32.goods_dim") // 指定一个或多个数据库表,需指定带库名的表名
                .username("root")
                .password("123456")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        //相应支持ExactlyOnce，必须开启Checkpoint
        env.enableCheckpointing(3000);

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(4) //设置Source的并行度，即读取数据对应的subtask的数量
                .print()
                .setParallelism(1); //设置sink的并行度

        env.execute("Print MySQL Snapshot + Binlog");

    }
}
