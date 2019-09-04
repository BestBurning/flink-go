package com.diyishuai.batch

import org.apache.flink.api.scala._

object FilterJob {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val dataSet = env.fromElements(1,2,3,4,5,6)
    dataSet.filter(_ > 3).print()
    //4 5 6

  }

}
