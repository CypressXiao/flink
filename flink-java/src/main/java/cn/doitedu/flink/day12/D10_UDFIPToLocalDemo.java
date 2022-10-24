package cn.doitedu.flink.day12;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day12
 * @className: D10_UDFIPToLocalDemo
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/9/9 19:29
 * @version: 1.0
 */

public class D10_UDFIPToLocalDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //在客户端注册一个可以Cache的文件,通过网络发送给TaskManager（类似spark的广播变量，广播不变得数据）
        env.registerCachedFile("/Users/start/Desktop/dev/flink-in-action/data/ip.txt", "ip-rules");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //106.121.4.252
        //42.57.88.186
        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);

        tableEnv.createTemporaryView("t_lines", socketTextStream, $("ip"));

        //注册自定义函数，是一个UDF,输入一个IP地址，返回Row<省、市>
        tableEnv.createTemporarySystemFunction("ipToLocation", IpLocation.class);

        Table table = tableEnv.sqlQuery(
                "SELECT ip, ipToLocation(ip) location FROM t_lines");

        DataStream<Row> res = tableEnv.toDataStream(table);

        res.print();

        env.execute();
    }

    public static class IpLocation extends ScalarFunction {

        private List<Tuple4<Long, Long, String, String>> lines = new ArrayList<>();

        //open方法是生命周期方法，会在调用evel方法前，调用一次
        @Override
        public void open(FunctionContext context) throws Exception {

            //获取缓存的文件（在TaskManager中获取缓存的数据）
            File cachedFile = context.getCachedFile("ip-rules");

            BufferedReader bufferedReader = new BufferedReader(
                    new InputStreamReader(new FileInputStream(cachedFile)));

            String line = null;
            //将数据整理好，添加到ArrayList（内存中了）
            while ((line = bufferedReader.readLine()) != null) {
                String[] fields = line.split("[|]");
                Long startNum = Long.parseLong(fields[2]);
                Long endNum = Long.parseLong(fields[3]);
                String province = fields[6];
                String city = fields[7];
                lines.add(Tuple4.of(startNum, endNum, province, city));
            }
        }

        //方法名必须叫eval
        public String eval(String ip) {
            Long ipNum = ip2Long(ip);
            return binarySearch(ipNum);
        }


//    public TypeInformation<Row> getResultType() {
//        return Types.ROW_NAMED(new String[]{"province", "city"}, Types.STRING, Types.STRING);
//    }

        private Long ip2Long(String ip) {
            String[] fragments = ip.split("[.]");
            Long ipNum = 0L;
            for (int i = 0; i < fragments.length; i++) {
                ipNum = Long.parseLong(fragments[i]) | ipNum << 8L;
            }
            return ipNum;
        }

        /**
         * 二分法查找
         */
        private String binarySearch(Long ipNum) {

            //Row result = null;
            String result = null;
            int index = -1;
            int low = 0;//起始
            int high = lines.size() - 1; //结束

            while (low <= high) {
                int middle = (low + high) / 2;
                if ((ipNum >= lines.get(middle).f0) && (ipNum <= lines.get(middle).f1))
                    index = middle;
                if (ipNum < lines.get(middle).f0)
                    high = middle - 1;
                else {
                    low = middle + 1;
                }
            }
            if (index != -1) {
                Tuple4<Long, Long, String, String> tp4 = lines.get(index);
                //result = Row.of(tp4.f2, tp4.f3);
                result = tp4.f2;
            }
            return result;
        }


    }
}
