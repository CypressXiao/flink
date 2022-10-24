package cn.doitedu.flink.day07;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day07
 * @className: D01_MapStateDemo1
 * @author: Cypress_Xiao
 * @description: KeyedState分为三种
 * valueState: Map<KEY,Value> value可以是多种类型,比如Integer,HashSet,HashMap
 * MapState: Map<KEY,map<k,v>> value是一个map
 * ListState: Map<KEY,list<v>> value是一个list
 * @date: 2022/9/2 10:33
 * @version: 1.0
 */

public class D01_MapStateDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //将同一个省份的数据分到一个分区内,并且按照城市累加,并且统计同一个省份的总金额
        //辽宁省,沈阳市,1000
        //辽宁省,大连市,2000
        //辽宁省,沈阳市,1000
        //辽宁省,大连市,2000
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, Double>> tpStream = lines.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], Double.parseDouble(fields[2]));
            }
        });

        KeyedStream<Tuple3<String, String, Double>, String> keyedStream = tpStream.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple4<String, Double, String, Double>> res = keyedStream.process(new IncomeCountFunction());
        res.print();
        env.execute();


    }

    public static class IncomeCountFunction extends KeyedProcessFunction<String, Tuple3<String, String, Double>, Tuple4<String, Double, String, Double>> {
        MapState<String, Double> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //Map<城市,金额>
            MapStateDescriptor<String, Double> mapDescriptor = new MapStateDescriptor<>("income-state", String.class, Double.class);
            //初始化或恢复状态
            mapState = getRuntimeContext().getMapState(mapDescriptor);

        }

        @Override
        public void processElement(Tuple3<String, String, Double> value, Context ctx, Collector<Tuple4<String, Double, String, Double>> out) throws Exception {
            //累加城市和省份的结果
            Double sum1 = 0.0;
            Double money = mapState.get(value.f1);
            if (money == null) {
                money = 0.0;
            }
            money += value.f2;
            mapState.put(value.f1, money);

            for (Double num : mapState.values()) {
                sum1 += num;
            }

            out.collect(Tuple4.of(value.f1, money, ctx.getCurrentKey(), sum1));
        }
    }

}
