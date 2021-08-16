package com.atguigu.spark.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-06-02 16:43
 */
object Test_Cache {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    sc.setCheckpointDir("output")

    val rdd = sc.makeRDD(List("Hello", "Hello", "Hello", "Hello", "Hello", "Spark", "Spark","Hive","Hive"),2)

    val rdd1 = rdd.map((_,1))

    println(rdd1.toDebugString)

    val rdd2 = rdd1.map(_._1)

    println(rdd2.toDebugString)

    rdd1.checkpoint()

    rdd2.collect().foreach(println)



    println("---------------------")


    println(rdd1.toDebugString)

    println("---------------------")

    println(rdd2.toDebugString)

    rdd2.collect().foreach(println)

    Thread.sleep(10000000)

    //关闭sc
    sc.stop()
  }
}
