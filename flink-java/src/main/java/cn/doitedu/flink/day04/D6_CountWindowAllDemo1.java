package cn.doitedu.flink.day04;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day04
 * @className: D6_CountWindowAllDemo1
 * @author: Cypress_Xiao
 * @description: 按照数据的条数划分窗口,窗口就是将无限流化为一个个有限的数据流的手段,然后对每个小的有限数据流依次进行处理
 * 窗口的分类:按照数据条数,按照时间划分的窗口  按照是否keyBy可以划分为keyedWindow(先keyBy再划分窗口)
 * 和NonKeyedWindow(没有keyBy就划分窗口,并行度为1,生产中很少使用,因为可能出现数据倾斜)
 * @date: 2022/8/28 15:41
 * @version: 1.0
 */

public class D6_CountWindowAllDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        //字符串转化为数字
        SingleOutputStreamOperator<Integer> numStream = lines.map(Integer::parseInt);
        //没有keyBy就划分窗口
        AllWindowedStream<Integer, GlobalWindow> windowedStream = numStream.countWindowAll(5);
        //划分窗口后要调用window算子,对window里面的数据进行处理
        SingleOutputStreamOperator<Integer> res = windowedStream.sum(0);
        res.print();
        env.execute();


    }
}
