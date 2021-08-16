package com.atguigu.spark.day01

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-05-31 14:53
 */
object Spark_Core_day01_4 {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val rdd1=sc.makeRDD(1 to 4)

    val rdd2=sc.makeRDD(3 to 6)

    rdd1.zip(rdd2).foreach(println)

    //关闭sc
    sc.stop()
  }
}
