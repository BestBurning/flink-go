package com.diyishuai.stream.datasource

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

/**
  * @author: Bruce
  * @date: 2019-09-01
  * @description:
  */
object MyDataSource {

  def main(args: Array[String]): Unit = {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream = env.addSource(new RichSourceFunction[Student]() {
      override def run(sourceContext: SourceFunction.SourceContext[Student]): Unit = {
        while (true){
          for (i <- 1 to 10) {
            sourceContext.collect(new Student("s"+i,i%10))
          }
          Thread.sleep(10000)
        }
      }

      override def cancel(): Unit = {

      }
    })

    dataStream.map{s => new Student(s.name+" hi",s.age)}
        .addSink(new SinkFunction[Student] {
          override def invoke(value: Student, context: SinkFunction.Context[_]): Unit = {
            println(value)
          }
        })


    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }

}

case class Student(val name: String,val age: Int){}
