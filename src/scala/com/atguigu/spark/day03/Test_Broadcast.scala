package com.atguigu.spark.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-06-02 18:01
 */
object Test_Broadcast {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List("Hello", "Hello", "Hello", "Hello", "Hello", "Spark", "Spark","Hive","Hive"),2)

    val value = sc.broadcast("H")

    rdd.filter(_.contains(value)).collect().foreach(println)



    //关闭sc
    sc.stop()
  }
}
