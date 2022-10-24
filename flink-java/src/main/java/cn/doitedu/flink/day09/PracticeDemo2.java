package cn.doitedu.flink.day09;

import lombok.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Map;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day09
 * @className: PracticeDemo2
 * @author: Cypress_Xiao
 * @description: 定时器实现topN的问题
 * @date: 2022/9/6 9:32
 * @version: 1.0
 */

public class PracticeDemo2 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);

        SingleOutputStreamOperator<String> watermarks = lines.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String element) {
                String[] fields = element.split(",");
                return Integer.parseInt(fields[0]);
            }
        });

        SingleOutputStreamOperator<Tuple3<String, String, String>> tpStream = watermarks.map(new MapFunction<String, Tuple3<String, String, String>>() {
            //这里不是直接调用sum,reduce等算子,所以不用用tuple中含1的形式
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[3], fields[2], fields[4]);
            }
        });

        KeyedStream<Tuple3<String, String, String>, Tuple2<String, String>> keyedStream = tpStream.keyBy(new KeySelector<Tuple3<String, String, String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple3<String, String, String> value) throws Exception {
                return Tuple2.of(value.f0, value.f1);
            }
        });

        SingleOutputStreamOperator<ResultBean> res = keyedStream.process(new MyKeyedProcessFunction());

        res.print();

        env.execute();


    }

    private static class MyKeyedProcessFunction extends KeyedProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, ResultBean> {
        private transient MapState<String, Integer> mapState;
        private long watermark = 0;
        private long trigger = 0;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Integer> descriptor = new MapStateDescriptor<String, Integer>("map-state", String.class, Integer.class);
            mapState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(Tuple3<String, String, String> value, Context ctx, Collector<ResultBean> out) throws Exception {
            Integer count = mapState.get(value.f2);
            if (count == null) {
                count = 0;
            }
            count += 1;
            mapState.put(value.f2, count);
            watermark = ctx.timerService().currentWatermark();
            trigger = watermark - watermark % 10000 + 10000;
            ctx.timerService().registerEventTimeTimer(trigger);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<ResultBean> out) throws Exception {
            Tuple2<String, String> key = ctx.getCurrentKey();
            ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : mapState.entries()) {
                list.add(Tuple2.of(entry.getKey(), entry.getValue()));
            }
            list.sort((o1, o2) -> (o2.f1 - o1.f1));
            for (int i = 0; i < Math.min(3, list.size()); i++) {
                Tuple2<String, Integer> tp = list.get(i);
                out.collect(new ResultBean(key.f0, key.f1, tp.f0, tp.f1, i + 1, watermark, trigger));
            }
            mapState.clear();
        }
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    public static class ResultBean {
        public String cid; //商品分类ID
        public String eid; //事件ID
        public String pid; //商品ID
        public int counts; //次数
        public int ord;    //排列顺序
        public long winStart;
        public long winEnd;

    }
}
