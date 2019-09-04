package com.diyishuai.batch

import org.apache.flink.api.scala._

object FlatmapJob {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

//    val dataSet = env.fromElements("hi asd askdn a  asdn n","asjdb asjd hi asdj")
//    val words = dataSet
//      .flatMap(_.split(" "))
//      .map((_,1))
//      .groupBy(0)
//      .sum(1)

    val dataSet = env.fromElements("ABC","DEF")

//    val words = dataSet.map(_.toLowerCase)
//    abc
//    def

    dataSet.flatMap(_.toLowerCase).print()
    //    a
    //    b
    //    c
    //    d
    //    e
    //    f




//    words.print()

  }

}
