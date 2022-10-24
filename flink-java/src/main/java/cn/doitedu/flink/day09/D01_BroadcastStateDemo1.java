package cn.doitedu.flink.day09;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day09
 * @className: D01_BroadcastStateDemo1
 * @author: Cypress_Xiao
 * @description: Flink广播状态的使用场景:1.将可变的维度数据广播出去,然后可以实现高效的关联维度数据
 * 优点:高效,维度数据可以实时改变
 * 缺点:要广播的维度数据不能太大
 * @date: 2022/9/5 10:25
 * @version: 1.0
 */

public class D01_BroadcastStateDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        //c10,手机,insert
        //c11,图书,insert
        //c12,服装,insert
        //c13,家具,insert
        //c13,实木家具,update
        //c12,服装,delete
        DataStreamSource<String> categoryLinesStream = env.socketTextStream("hadoop001", 8888);
        SingleOutputStreamOperator<Tuple3<String, String, String>> categoryTpStream = categoryLinesStream.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], fields[2]);
            }
        });

        //将分类维度数据,广播出去,下游以map方式保存
        //定义状态描述器,描述的是要广播的数据,广播后在下游以何种方式存储(以什么样的状态保存起来)
        MapStateDescriptor<String, String> stateDescriptor = new MapStateDescriptor<>("category-state", String.class, String.class);
        //将维度数据流广播,并且在下游以mapState的形式存储
        BroadcastStream<Tuple3<String, String, String>> broadcastStream = categoryTpStream.broadcast(stateDescriptor);

        //处理事实数据(用户行为数据)
        //o01,c10,2000
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 9999);
        SingleOutputStreamOperator<Tuple3<String, String, Double>> tpStream = lines.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], Double.parseDouble(fields[2]));
            }
        });

        //将事实数据关联维度数据
        SingleOutputStreamOperator<Tuple4<String, String, String, Double>> res = tpStream.connect(broadcastStream).process(new BroadcastProcessFunction<Tuple3<String, String, Double>, Tuple3<String, String, String>, Tuple4<String, String, String, Double>>() {
            //处理非广播的数据
            @Override
            public void processElement(Tuple3<String, String, Double> value, ReadOnlyContext ctx, Collector<Tuple4<String, String, String, Double>> out) throws Exception {
                String cid = value.f1;
                //根据分类id到状态中取数据
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(stateDescriptor);
                out.collect(Tuple4.of(value.f0, value.f1, broadcastState.get(cid), value.f2));
            }

            //处理广播的数据
            @Override
            public void processBroadcastElement(Tuple3<String, String, String> value, Context ctx, Collector<Tuple4<String, String, String, Double>> out) throws Exception {
                String cid = value.f0;
                String name = value.f1;
                String type = value.f2;
                //根据状态描述器获取状态
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(stateDescriptor);
                if ("delete".equals(type)) {
                    broadcastState.remove(cid);
                } else {
                    broadcastState.put(cid, name);
                }

            }
        });

        res.print();
        env.execute();


    }
}
