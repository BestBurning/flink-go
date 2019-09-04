package com.diyishuai.batch

import org.apache.flink.api.scala._

object MapJob {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.fromElements((1,2),(3,4))
    val sums =  data.map{nums => nums._1 + nums._2}
    sums.print()
  }

}
