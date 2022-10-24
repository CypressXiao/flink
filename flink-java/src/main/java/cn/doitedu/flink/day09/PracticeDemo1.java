package cn.doitedu.flink.day09;


import lombok.*;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;



import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day09
 * @className: PracticeDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/9/5 9:11
 * @version: 1.0
 */

public class PracticeDemo1 {
    public static void main(String[] args) throws Exception {
        /**
         * #时间,用户id,事件类型,商品分类ID,商品ID,金额
         * 1000,u001,view,c10,p1001,2000
         * 1100,u002,view,c10,p1001,2000
         * 1200,u002,view,c10,p1001,2000
         * 1300,u002,view,c11,p1011,2000
         * 2400,u002,addCart,c10,p1001,2000
         * 2500,u002,view,c10,p1001,2000
         * 2500,u003,pay,c12,p1012,2000
         * 2100,u004,view,c10,p1001,2000
         * 2200,u005,addCart,c10,p1001,2000
         * 2300,u005,view,c13,p1013,2000
         * 2400,u007,focus,c10,p1001,2000
         * 2500,u008,view,c18,p2008,2000
         *
         * 统计一段时间的各种商品分类、各种事件类型的热门商品topN
         * 按商品分类,事件类型分区,区内按商品id自动分组
         */

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

        WindowedStream<Tuple3<String, String, String>, Tuple2<String, String>, TimeWindow> windowedStream = keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)));

        SingleOutputStreamOperator<ResultBean> res = windowedStream.aggregate(new MyAggFunction(), new MyProcessFunction());

        res.print();

        env.execute();


    }

    private static class MyAggFunction implements AggregateFunction<Tuple3<String, String, String>, Map<String, Integer>, Map<String, Integer>> {
        //里面的方法都是一个key调用一次,即分区里的一个分组调用一次
        @Override
        public Map<String, Integer> createAccumulator() {
            return new HashMap<>();
        }


        @Override
        public Map<String, Integer> add(Tuple3<String, String, String> value, Map<String, Integer> accumulator) {
            accumulator.put(value.f2, accumulator.getOrDefault(value.f2, 0) + 1);
            return accumulator;
        }

        @Override
        public Map<String, Integer> getResult(Map<String, Integer> accumulator) {
            return accumulator;
        }

        @Override
        public Map<String, Integer> merge(Map<String, Integer> a, Map<String, Integer> b) {
            return null;
        }
    }

    private static class MyProcessFunction extends ProcessWindowFunction<Map<String, Integer>, ResultBean, Tuple2<String, String>, TimeWindow> {
        //一个key对应一个map,不是一个分区一个map
        @Override
        public void process(Tuple2<String, String> key, Context context, Iterable<Map<String, Integer>> elements, Collector<ResultBean> out) throws Exception {
            Map<String, Integer> mp = elements.iterator().next();
            ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : mp.entrySet()) {
                list.add(Tuple2.of(entry.getKey(), entry.getValue()));
            }
            list.sort((o1, o2) -> (o2.f1 - o1.f1));
            for (int i = 0; i < Math.min(3, list.size()); i++) {
                Tuple2<String, Integer> tp = list.get(i);
                out.collect(new ResultBean(key.f0,key.f1,tp.f0,tp.f1,i+1,context.window().getStart(),context.window().getEnd()));
            }
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

