package cn.doitedu.flink.day09;

import lombok.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import java.time.Duration;
import java.util.ArrayList;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day09
 * @className: PracticeDemo3
 * @author: Cypress_Xiao
 * @description: 通过定时器, 实现滑动窗口的topN
 * @date: 2022/9/6 20:06
 * @version: 1.0
 */

public class PracticeDemo3 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         * #时间,用户id,事件类型,商品分类ID,商品ID,金额
         * 1000,u001,view,c10,p1001,2000
         * 1100,u002,view,c10,p1001,2000
         */
        DataStreamSource<String> lines = env.socketTextStream("hadoop001", 8888);

        //因为是eventTime所有要定义水位线
        SingleOutputStreamOperator<String> watermarks = lines.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String element, long recordTimestamp) {
                String[] fields = element.split(",");
                return Long.parseLong(fields[0]);
            }
        }));

        //对数据进行处理,并且按照商品分类,事件类型,商品id进行分区,三个维度分区之后需要再按需求聚合
        SingleOutputStreamOperator<Tuple3<String, String, String>> tpStream = watermarks.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[3], fields[2], fields[4]);
            }
        });

        KeyedStream<Tuple3<String, String, String>, Tuple3<String, String, String>> keyedStream = tpStream.keyBy(t -> t);

        //进行窗口处理,因为要实现滑动窗口用定时器不好实现
        WindowedStream<Tuple3<String, String, String>, Tuple3<String, String, String>, TimeWindow> windowedStream = keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)));

        //对窗口里的数据进行处理
        SingleOutputStreamOperator<ResultBean> aggStream = windowedStream.aggregate(new MyAggFunction(), new MyProcessWindowFunction());

        //然后在再分区,再使用processFunction使用定时器进行排序
        KeyedStream<ResultBean, Tuple2<String, String>> keyedStream1 = aggStream.keyBy(t -> Tuple2.of(t.cid, t.eid));

        SingleOutputStreamOperator<ResultBean> res = keyedStream1.process(new MyKeyedProcessFunction());

        res.print();

        env.execute();

    }

    private static class MyKeyedProcessFunction extends KeyedProcessFunction<Tuple2<String, String>, ResultBean, ResultBean> {
        private transient ListState<ResultBean> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<ResultBean> descriptor = new ListStateDescriptor<>("bean-state", ResultBean.class);
            listState = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void processElement(ResultBean value, Context ctx, Collector<ResultBean> out) throws Exception {
            //同一个窗口的数据先攒起来
            listState.add(value);
            long triggerTime = value.winEnd + 1;
            //如果waterMark大于等于valueEnd,当前窗口的数据就要排序输出
            ctx.timerService().registerEventTimeTimer(triggerTime);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<ResultBean> out) throws Exception {
            ArrayList<ResultBean> list = (ArrayList<ResultBean>) listState.get();
            list.sort((o1, o2) -> (o2.counts - o1.counts));
            for (int i = 0; i < Math.max(3, list.size()); i++) {
                ResultBean resultBean = list.get(i);
                resultBean.ord = i + 1;
                out.collect(resultBean);
            }

        }
    }

    private static class MyAggFunction implements AggregateFunction<Tuple3<String, String, String>, Integer, Integer> {

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple3<String, String, String> value, Integer accumulator) {
            accumulator += 1;
            return accumulator;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return null;
        }
    }

    private static class MyProcessWindowFunction extends ProcessWindowFunction<Integer, ResultBean, Tuple3<String, String, String>, TimeWindow> {

        @Override
        public void process(Tuple3<String, String, String> key, Context context, Iterable<Integer> elements, Collector<ResultBean> out) throws Exception {
            Integer count = elements.iterator().next();
            String cid = key.f0;
            String eid = key.f1;
            String pid = key.f2;
            long start = context.window().getStart();
            long end = context.window().getEnd();
            out.collect(new ResultBean(cid, eid, pid, count, 0, start, end));
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
