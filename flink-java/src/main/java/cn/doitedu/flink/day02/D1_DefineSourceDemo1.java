package cn.doitedu.flink.day02;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.UUID;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day02
 * @className: D1_DefineSourceDemo1
 * @author: Cypress_Xiao
 * @description: 自定义source
 * @date: 2022/8/25 9:18
 * @version: 1.0
 */

public class D1_DefineSourceDemo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //指定本地webUI的端口
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        DataStreamSource<String> numStream = env.addSource(new MySource01());
        /**
         * 直接实现了SourceFunction接口的Source的并行度为1,即是非并行的Source
         * 如果run方法中没有while循环,则run方法会退出,而不是一直运行,那么就是有限数据流
         */
        int num = numStream.getParallelism();

        System.out.println(num);
        numStream.print();

        env.execute();

    }

    //sourceFunction即输出的数据类型
    public static class MySource01 implements SourceFunction<String> {
        private boolean flag = true;

        /**
         * @param ctx:
         * @return void
         * @author Cypress_Xiao
         * @description source初始化完成后, 在run方法中产生数据
         * @date 2022/8/25 9:32
         */
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            System.out.println("run method invoked!!!");
            /*for (int i = 0; i < 1000; i++) {
                //使用SourceContext将数据输出,给后序的算子调用
                ctx.collect(i + "");
            }*/
            while (flag) {
                String s = UUID.randomUUID().toString();
                System.out.println(s);
                ctx.collect(s);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            System.out.println("cancel method invoked!!!");
            flag = false;
        }
    }
}
