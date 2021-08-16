package com.atguigu.spark.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-06-01 20:53
 */
object Test07 {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)// 一个Application

    val rdd = sc.makeRDD(Array(1,2,3,4,5,6,7,8,9,10),4)

    val rdd1 = rdd.map(_+1)
    println(rdd1.toDebugString)
    println("---------------------------------")

    val rdd2 = rdd1.groupBy(_%2==0,2)
    println(rdd2.toDebugString)
    println("---------------------------------")

    val rdd4 = rdd2.flatMap(x=>x._2)

    println(rdd4.toDebugString)
    println("---------------------------------")

    val rdd6 = rdd4.groupBy(_>5,4)
    println(rdd6.toDebugString)
    println("---------------------------------")
    val rdd7 = rdd6.mapValues(_.sum)
    println(rdd7.toDebugString)
    println("---------------------------------")
    var rdd8=rdd7.reduceByKey(_+_,1)
    println(rdd8.toDebugString)
    println("---------------------------------")
    //五个stage
    //两个Job
    rdd8.collect().foreach(println)

    println(rdd8.count())

    Thread.sleep(100000000)

    //关闭sc
    sc.stop()
  }
}
