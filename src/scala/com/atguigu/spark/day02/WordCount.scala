package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-05-31 21:16
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello world","hello hadoop","hello spark"),2)

    rdd.flatMap(_.split(" ").map((_,1)))
//      .reduceByKey(_+_).collect().foreach(println)
//        .foldByKey(0)(_+_).collect().foreach(println)
//        .aggregateByKey(0)(_+_,_+_).collect().foreach(println)
        .combineByKey(x=>x,(x:Int,y)=>x+y,(x:Int,y:Int)=>x+y).collect().foreach(println)

    //关闭sc
    sc.stop()
  }
}
