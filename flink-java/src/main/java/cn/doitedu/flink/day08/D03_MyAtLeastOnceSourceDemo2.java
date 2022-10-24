package cn.doitedu.flink.day08;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day08
 * @className: D03_MyAtLeastOnceSourceDemo2
 * @author: Cypress_Xiao
 * @description: 停止job不建议在页面中点击cancel按钮而是使用命令方式停止 bin/flink stop -p 指定savepoint目录 job的id(可以到页面找)
 * checkpoint和savepoint的区别
 * checkpoint是指固定时间将状态保存至指定的目录,savepoint是人为手动停止job时,手动指定目录保存状态;savepoint的数据可能会比checkpoint会更新一点
 * 因为手动调用savepoint会将最新的状态保存
 * 使用命令方式提交job并指定savepoint路径(-s 指定savepoint或checkpoint路径,但是不是具体到保存的state文件而是上一级目录即medata上一级)
 * @date: 2022/9/3 16:24
 * @version: 1.0
 */

public class D03_MyAtLeastOnceSourceDemo2 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(4);
        env.enableCheckpointing(5000);
        //CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION 默认的,当job取消后checkpoint会被删除
        //CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION 建议加上这个,当job被cancel后,外部保留的checkpoint数据会保留不会被删除
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStreamSource<String> lines = env.addSource(new MyAtLeastOnceSource("C:\\Users\\Cypress_Xiao\\Desktop\\data"));

        DataStreamSource<String> lines1 = env.socketTextStream("hadoop001", 8888);
        SingleOutputStreamOperator<String> res = lines1.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                if (value.startsWith("error")) {
                    throw new RuntimeException("数据错误,抛出异常!");
                }
                return value;
            }
        });

        DataStream<String> res1 = res.union(lines);

        res1.print();
        env.execute();


    }

    //要在该source中,使用operatorState,必须要实现CheckpointedFunction接口
    private static class MyAtLeastOnceSource extends RichParallelSourceFunction<String> implements CheckpointedFunction {
        //transient修饰的变量在subtask序列化时该字段不参与序列化,不会被保存起来,反序列化之后就是null
        //建议所有的状态(keyedState,operatorState都用transient修饰)
        private transient ListState<Long> offsetState; //是operatorState
        //执行顺序initializeState ->open ->run
        //开启checkpoint后,snapshotState才会执行,每次checkpoint时,每个subtask都会执行一次
        private boolean flag = true;
        private long offset = 0;
        private String dirPath;

        public MyAtLeastOnceSource(String filePath) {
            this.dirPath = filePath;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            //将最新的数据放到状态中
            offsetState.clear();
            offsetState.add(offset);

        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            //定义状态描述器
            ListStateDescriptor<Long> descriptor = new ListStateDescriptor<Long>("offset-state", Long.class);
            offsetState = context.getOperatorStateStore().getListState(descriptor);
            if (context.isRestored()) {//判断状态是否全部恢复完
                for (Long o : offsetState.get()) {
                    offset = o;
                }
            }

        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            String filePath = dirPath+"/"+getRuntimeContext().getIndexOfThisSubtask()+".txt";
            //读取对应文件名称的数据
            RandomAccessFile raf = new RandomAccessFile(filePath, "r");//指定文件的路径和处理方式,w代表写,r代表读
            //从指定偏移量读取数据
            raf.seek(offset);
            //不停的循环读取
            while (flag) {
                //RandomAccessFile默认的读取字符集为ISO-8859-1,读取中文会有乱码
                String line = raf.readLine();
                if (line != null) {
                    line = new String(line.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
                    ctx.collect(line);
                    //获取最新的偏移量,并赋值给offset
                    offset = raf.getFilePointer();
                } else {
                    Thread.sleep(5000);
                }
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }
}
