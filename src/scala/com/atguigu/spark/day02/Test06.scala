package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-05-31 18:44
 */
object Test06 {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello world","hello hadoop","hello spark"),2)
    /*
    groupBy() 按照逻辑对数据进行分区,会走shuffle,返回值是一个二元组,第一位是key,第二位是各个值组成的集合

     */
    rdd.flatMap(_.split(" ")
      .map((_,1)))
      .groupBy(_._1)
      .mapPartitionsWithIndex((x,y)=>y.map((x,_))
      ).foreach(println)
    /*
    sortBy() 根据传入的函数逻辑对数据进行排序
    第一个参数是排序逻辑
    第二个参数是正序还是倒序,true为正序,false为倒序
    第三个参数是分区数,默认为原分区数.
    sortBy是走shuffle的
     */
//    rdd.flatMap(_.split(" ")).sortBy(_.charAt(0),false,3).mapPartitionsWithIndex((x,y)=>y.map((x,_))).foreach(println)


    /*
    pipe(脚本路径) 传入一个脚本路径,每个分区执行一次脚本,返回输出的RDD
     */
    Thread.sleep(55555555)
    //关闭sc
    sc.stop()
  }
}
