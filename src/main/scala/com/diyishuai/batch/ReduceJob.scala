package com.diyishuai.batch

import org.apache.flink.api.scala._

object ReduceJob {


  def main(args: Array[String]): Unit = {
    val env  = ExecutionEnvironment.getExecutionEnvironment
    val words: DataSet[WC] = env.fromElements(
      new WC("hi",1),
      new WC("haha",3),
      new WC("abc",4),
      new WC("abc",9)
    )
    // [...]

//    val wordCounts = words.groupBy("word").reduce {
//      (w1, w2) => new WC(w1.word, w1.count + w2.count)
//    }.print()

    val wordCounts = words.groupBy(_.word).reduce {
      (w1, w2) => new WC(w1.word, w1.count + w2.count)
    }.print()
  }


}

case class WC(val word: String, val count: Int) {
  def this() {
    this(null, -1)
  }
  // [...]

}