package com.diyishuai.batch

import org.apache.flink.api.scala._

object SumJob {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val dataSet = env.fromElements(1,1,2,3,4,5,6)
    dataSet.map(num => (num,num,1)).sum(2).print()

  }

}
