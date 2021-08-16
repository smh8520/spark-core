package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-05-31 18:07
 */
object Test01 {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Array(1,2,3,4,5,6),2)
    //一次对一条数据进行映射
    rdd.map(x=>x+2).collect().foreach(println)
    //一次对一个分区的数据进行映射
    rdd.mapPartitions(x=>x.map(_+2)).collect().foreach(println)
    //一次对一个分区的数据进行映射,带分区号
    rdd.mapPartitionsWithIndex((x,y)=>y.map((x,_))).collect().foreach(println)

    /*
    map和mapPartitions的区别
    map一次对一条数据进行映射
    mapPartitions一次对一个分区的数据进行映射.
    mapPartitions的效率比较高,不过在当前分区的数据没有处理完之前是不会释放上一个RDD的资源的,所以分区数据过多的话可能
    会造成内存泄漏
    当资源充足的情况下优先使用mapPartitions
     */
    //关闭sc
    sc.stop()
  }
}
