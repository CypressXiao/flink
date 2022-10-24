package cn.doitedu.flink.day08;


import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;


/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day08
 * @className: D02
 * @author: Cypress_Xiao
 * @description: 自定义一个读取指定目录下的文件, 可以实现atLeastOnce语义;该程序为了练习使用OperatorState
 * 算子状态存储器中只有一个list数据结构的state
 * @date: 2022/9/3 14:50
 * @version: 1.0
 */

public class D02_OperatorStateDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(5000);

        DataStreamSource<String> lines = env.addSource(new MyAtLeastOnceSource("C:\\Users\\Cypress_Xiao\\Desktop\\data"));

        lines.print();
        env.execute();


    }

    //要在该source中,使用operatorState,必须要实现CheckpointedFunction接口
    private static class MyAtLeastOnceSource extends RichParallelSourceFunction<String> implements CheckpointedFunction {
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
            ListStateDescriptor<Long> descriptor = new ListStateDescriptor<>("offset-state", Long.class);
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
                    Thread.sleep(500);
                }
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }
}
