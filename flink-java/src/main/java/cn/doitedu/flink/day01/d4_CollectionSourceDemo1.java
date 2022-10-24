package cn.doitedu.flink.day01;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day01
 * @className: ParallelCollectionSourceDemo1
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/8/24 16:45
 * @version: 1.0
 */

public class d4_CollectionSourceDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //DataStreamSource<Integer> nums = env.fromElements(1,2,3,4,5,6,7,8);
        DataStreamSource<Integer> nums = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8));
        nums.print();
        System.out.println(nums.getParallelism());
        env.execute();
    }
}
