package com.atguigu.spark.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-05-31 11:20
 */
object Spark_Core_day01_3 {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(Array(1,2,3,4,5,6,7,8,9,10),2)
    rdd.glom().collect().foreach(x=>println(x.mkString(",")))
    rdd.sortBy(x=>x,true,5)
        .mapPartitionsWithIndex((x,y)=>y.map((_,x))).collect().foreach(println)



    //关闭sc
    sc.stop()
  }
}
