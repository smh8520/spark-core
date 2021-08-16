package com.atguigu.spark.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-05-29 14:06
 */
object Spark_Core_day01_1 {
  def main(args: Array[String]): Unit = {

    //创建配置文件对象
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //创建sc对象
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("D:\\workspace\\spark-core\\input",3)


    rdd.mapPartitionsWithIndex((x,y)=>y.flatMap(_.split(" ")).map((x,_))).saveAsTextFile("D:\\workspace\\spark-core\\output\\o8")


    //关闭资源
    sc.stop()
  }
}
