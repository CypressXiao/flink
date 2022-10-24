package cn.doitedu.flink

import org.apache.flink.streaming.api.scala._

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink
 * @objectName: d1_StreamWordCount
 * @author: Cypress_Xiao
 * @description: scala版本的wordCount
 * @date: 2022/8/24 11:55
 * @version: 1.0
 */
object d1_StreamWordCount {
  def main(args:Array[String]):Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val line:DataStream[String] = env.socketTextStream("hadoop001", 8899)

    val res:DataStream[(String, Int)] = line.
      flatMap(words => {
        words.split("\\s+").map((_, 1))
      })
      .keyBy(_._1)
      .sum(1)

    res.print()

    env.execute();




  }

}
