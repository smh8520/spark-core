package com.atguigu.spark.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-05-31 18:15
 */
object Test02 {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Array("hello word","he she it","bye good"),2)

    //对数据映射之后再扁平化,要求映射的返回值为集合
    rdd.flatMap(x=>x.split(" ")).collect().foreach(println)

    //关闭sc
    sc.stop()
  }
}
