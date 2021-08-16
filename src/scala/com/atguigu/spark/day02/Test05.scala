package com.atguigu.spark.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-05-31 18:36
 */
object Test05 {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Array(1,5,2,4,5,2,1,4,7,5,6,9,7,5,3,6,5,8),2)
    /*
    distinct() 对数据进行去重.如果不填参数的话,默认还是之前的分区,这样是没有shuffle的
    如果填参数的话,则会根据参数从新进行分区,这样是走shuffle的
    */
    rdd.distinct(3).mapPartitionsWithIndex((x,y)=>y.map((x,_))).foreach(println)

    /*
    filter() 根据传入的函数逻辑对数据进行过滤,不满足条件的都过滤掉
     */

    Thread.sleep(55555555)
    //关闭sc
    sc.stop()
  }
}
