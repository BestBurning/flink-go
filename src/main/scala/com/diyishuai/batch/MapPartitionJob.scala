package com.diyishuai.batch

import org.apache.flink.api.scala._

object MapPartitionJob {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val dataSet = env.fromElements(
      "hi hi asdjasd ad naksd",
      "dasdh hi asdkjasdhi ad",
      "asdjh hi asdkad ad",
      "hi ok"
    )
    //val sizes = dataSet.map{_.size}
    //22
    //22
    //18
    //5



    val sizes = dataSet.mapPartition{in => Some(in.size)}
    sizes.print()
    //4
  }

}
