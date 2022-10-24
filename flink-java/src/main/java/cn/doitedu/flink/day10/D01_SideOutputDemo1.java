package cn.doitedu.flink.day10;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day10
 * @className: D01_SideOutputDemo1
 * @author: Cypress_Xiao
 * @description: sideOutput是将数据流中的数据按照类型打上标签,以后按照标签筛选出想要的数据
 * @date: 2022/9/6 11:10
 * @version: 1.0
 */

public class D01_SideOutputDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //1
        //2
        //3
        //abc
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        //定义标签
        OutputTag<Integer> oddTag = new OutputTag<Integer>("odd-tag") { };
        OutputTag<Integer> evenTag = new OutputTag<Integer>("even-tag") { };
        OutputTag<String> strTag = new OutputTag<String>("str-tag") { };


        SingleOutputStreamOperator<String> classifyStream = lines.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                try {
                    int num = Integer.parseInt(value);
                    if (num % 2 == 0) {
                        //偶数,输出数据并关联对应的标签
                        ctx.output(evenTag, num);
                    } else {
                        //奇数
                        ctx.output(oddTag, num);
                    }
                } catch (NumberFormatException e) {
                    //字符串
                    ctx.output(strTag, value);
                }
            }
        });

        DataStream<Integer> evenStream = classifyStream.getSideOutput(evenTag);
        DataStream<Integer> oddStream = classifyStream.getSideOutput(oddTag);
        DataStream<String> strStream = classifyStream.getSideOutput(strTag);

        oddStream.print("odd");
        evenStream.print("even");
        strStream.print("str");

        env.execute();
    }
}
