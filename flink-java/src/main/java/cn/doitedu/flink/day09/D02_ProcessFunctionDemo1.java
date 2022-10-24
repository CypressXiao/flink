package cn.doitedu.flink.day09;


import com.alibaba.fastjson.JSON;
import lombok.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day09
 * @className: D02_ProcessFunctionDemo1
 * @author: Cypress_Xiao
 * @description: 是Flink流式计算底层的方法,可以访问flink底层的属性和方法
 * 有三种功能:1.对数据一条条处理;2.使用状态,但是只能针对keyedState;3.使用定时器,也是只能针对keyedState
 * @date: 2022/9/5 11:18
 * @version: 1.0
 */

public class D02_ProcessFunctionDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        //{"oid":"o001","cid":"c10","money":100.8}
        //{"oid":"o002","cid":"c10","money":100.8
        //{"oid":"o002","cid":"c11","money":200.8}
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);

        //使用processFunction将数据整理并过滤
        SingleOutputStreamOperator<OrderBean> res = lines.process(new ProcessFunction<String, OrderBean>() {
            @Override
            public void processElement(String value, Context ctx, Collector<OrderBean> out) throws Exception {
                try {
                    OrderBean orderBean = JSON.parseObject(value, OrderBean.class);
                    //输出数据
                    out.collect(orderBean);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        res.print();
        env.execute();

    }


}


@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
class OrderBean{
    public String oid;
    public String cid;
    public Double money;
}
