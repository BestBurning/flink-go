package com.diyishuai.batch

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import scala.collection.JavaConverters._

object BroadcastJob {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val toBroadcase = env.fromElements(1,2,3)

    val data = env.fromElements("hi","ok","go").map(
      new RichMapFunction[String,String]() {
        var broadcastSet: Traversable[String] = null

        override def open(config: Configuration): Unit = {
          // 3. Access the broadcast DataSet as a Collection
          broadcastSet = getRuntimeContext().getBroadcastVariable[String]("b").asScala
        }

        override def map(in: String): String = {
          in+" o" +broadcastSet
        }
      }
    ).withBroadcastSet(toBroadcase,"b")

    data.print()


  }
}
