package com.atguigu.spark.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-06-01 19:09
 */
object Test06 {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Array(1,2,3,4),5)


    println(rdd.reduce((x, y) => x + y))

    val i = rdd.aggregate(10)(_+_,_+_)
    println(i)

    println(rdd.fold(10)(_ -_))

    //关闭sc
    sc.stop()
  }
}
