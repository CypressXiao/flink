package cn.doitedu.flink.day07;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day07
 * @className: D02_ListStateDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/9/2 14:32
 * @version: 1.0
 */

public class D02_ListStateDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);
        //将同一个用户的多个行为按照先后顺序保存起来
        //u001,click
        //u001,addCart
        //u001,pay
        //u002,click
        KeyedStream<Tuple2<String, String>, String> keyedStream = lines.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], fields[1]);
            }
        }).keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple2<String, List<String>>> res = keyedStream.process(new MyListStateProcess());
        res.print();

        env.execute();

    }

    private static class MyListStateProcess extends KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String, List<String>>> {
        ListState<String> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<String> descriptor = new ListStateDescriptor<>("event-list", String.class);
            listState = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, List<String>>> out) throws Exception {
            listState.add(value.f1);
            ArrayList<String> list = (ArrayList<String>) listState.get();
            if(list.size()>10){
                list.remove(0);
            }
            out.collect(Tuple2.of(value.f0, list));
        }
    }

}
