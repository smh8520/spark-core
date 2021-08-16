package com.atguigu.spark.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-05-31 19:52
 */
object Test10 {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("D:\\workspace\\spark-core\\input\\agent.log")

    val rdd1 = rdd.map(x => {
      val strings = x.split(" ")

      (strings(1) + "_" + strings(4), 1)
    })
    val rdd2 = rdd1.reduceByKey(_+_)

    val rdd3 = rdd2.map(x => {
      val strings = x._1.split("_")
      (strings(0), (strings(1), x._2))
    })
    val rdd4 = rdd3.groupByKey()

    val rdd5 = rdd4.mapValues(x=>x.toList.sortWith(_._2>_._2).take(3))

    rdd5.collect().foreach(println)


    //关闭sc
    sc.stop()
  }
}
