package cn.doitedu.flink.day02;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.UUID;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day02
 * @className: D3_DefineParallelSourceDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/8/25 10:19
 * @version: 1.0
 */

public class D3_DefineParallelSourceDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //指定本地webUI的端口
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        DataStreamSource<String> numStream = env.addSource(new MyRichSource()).setParallelism(2);
        /**
         * 继承了RichParallelSourceFunction类的Source的并行度可以为多个,即是并行的Source
         * 可以重写两个方法open和close,这些方法顺序执行,一定会执行并且按照对应的顺序执行(生命周期方法)
         * 每个subtask中open,run,cancel,close执行一次
         */
        int num = numStream.getParallelism();

        System.out.println(num);
        numStream.print();

        env.execute();

    }

    //sourceFunction即输出的数据类型
    public static class MyRichSource extends RichParallelSourceFunction<String> {
        private boolean flag = true;

        @Override
        public void open(Configuration parameters) throws Exception {
            int index = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println("subtask "+index+" open method invoked!!!");
        }

        /**
         * @param ctx:
         * @return void
         * @author Cypress_Xiao
         * @description source初始化完成后, 在run方法中产生数据
         * @date 2022/8/25 9:32
         */
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            int index = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println("subtask "+index+" run method invoked!!!");
            /*for (int i = 0; i < 1000; i++) {
                //使用SourceContext将数据输出,给后序的算子调用
                ctx.collect(i + "");
            }*/
            while (flag) {
                String s = UUID.randomUUID().toString();
                ctx.collect(s);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            int index = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println("subtask "+index+" cancel method invoked!!!");
            flag = false;
        }

        @Override
        public void close() throws Exception {
            int index = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println("subtask "+index+" close method invoked!!!");
        }
    }
}
