package cn.doitedu.flink.day05;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink.day05
 * @className: D6_WindowStartTimeAndEndTimeDemo1
 * @author: Cypress_Xiao
 * @description: 窗口的起始时间和结束时间是对齐的,便于窗口的触发条件的计算,滚动窗口的起始时间,结束时间[start,end),
 * 都是窗口长度的整数倍
 * @date: 2022/8/30 15:11
 * @version: 1.0
 */

public class D6_WindowStartTimeAndEndTimeDemo1 {
    public static void main(String[] args) {
        long eventTime = 1661832001111L;
        //滑动窗口的长度是5秒
        int winSize = 5000;
        //计算起始时间
        long startTime = eventTime - eventTime % winSize;

        //计算结束时间
        long endTime = eventTime - eventTime % winSize + winSize;
        System.out.println("["+startTime+","+endTime+")");


    }
}
