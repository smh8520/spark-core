package com.atguigu.spark.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-06-01 18:52
 */
object Test05 {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Array("hello world","hello hadoop","hadoop spark"))

    val worldRdd = rdd.flatMap(_.split(" ")).map((_,1))

    println(worldRdd.dependencies)

    println(worldRdd.toDebugString)

    println(worldRdd.count())
    println(worldRdd.countByKey())
    println(rdd.flatMap(_.split(" ")).countByValue())

    worldRdd.take(3).foreach(println)

    worldRdd.takeOrdered(3)(Ordering[(String,Int)].reverse).foreach(println)

    worldRdd.foreach(println)
    println("------------------------------")
    worldRdd.collect().foreach(println)

    println(worldRdd.first())


    //关闭sc
    sc.stop()
  }
}
