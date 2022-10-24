package cn.doitedu.flink.day11;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day11
 * @className: D07_SQLAdCountDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/9/8 21:30
 * @version: 1.0
 */

public class D07_SQLAdCountDemo1 {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        //为了容错，必须开启checkpoint
        env.enableCheckpointing(15000);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<AdBean> tpStream = lines.map(new MapFunction<String, AdBean>() {
            @Override
            public AdBean map(String value) throws Exception {
                if (value.startsWith("error")) {
                    throw new RuntimeException("有错误数据,抛出异常");
                }
                String[] fields = value.split(",");
                return new AdBean(fields[0], fields[1], fields[2]);
            }
        });


        //注册视图
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //下面的两种创建视图的方法都过时
        //tEnv.createTemporaryView("v_ad_log", tpStream, "uid,eid,aid");
        //tEnv.createTemporaryView("v_ad_log", tpStream, $("uid"), $("eid"), $("aid"));

        tEnv.createTemporaryView("v_ad_log", tpStream);

        //TableResult tableResult = tEnv.executeSql("desc v_ad_log");

        TableResult tableResult = tEnv.executeSql("select aid, eid, count(*) counts, count(distinct(uid)) dis_counts from v_ad_log group by aid, eid");

        tableResult.print();

        env.execute();

    }


    public static class AdBean {

        public String uid;
        public String eid;
        public String aid;

        public AdBean() {}

        public AdBean(String uid, String eid, String aid) {
            this.uid = uid;
            this.eid = eid;
            this.aid = aid;
        }

        @Override
        public String toString() {
            return "AdBean{" +
                    "uid='" + uid + '\'' +
                    ", eid='" + eid + '\'' +
                    ", aid='" + aid + '\'' +
                    '}';
        }
    }
}
