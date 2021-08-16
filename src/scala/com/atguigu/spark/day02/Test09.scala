package com.atguigu.spark.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-05-31 19:28
 */
object Test09 {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(("hello world",4),("hello spark",3),("world hadoop",1),("spark",1)))


    //使用reduceByKey来求wordCount
//    rdd.flatMap(x=>x._1.split(" ").map((_,x._2))).reduceByKey(_+_).collect().foreach(println)

    //使用foldByKey来求wordCount
//    rdd.flatMap(x=>x._1.split(" ").map((_,x._2))).foldByKey(0)(_+_).collect().foreach(println)
    //使用aggregateByKey求wordCount
//    rdd.flatMap(x=>x._1.split(" ").map((_,x._2))).aggregateByKey(0)(_+_,_+_).collect().foreach(println)


    //使用combineByKey来求wordCount
    rdd.flatMap(x=>x._1.split(" ").map((_,x._2))).combineByKey(x=>x,(x:Int,y)=>x+y,(x:Int,y:Int)=>x+y)
        .collect().foreach(println)

    //reduceByKey和groupByKey的区别
    /*
    reduceByKey在分区之间聚合之前先对分区内的数据进行一次聚合
    groupByKey是直接在分区之间进行分组,直接进行shuffle
     */


    /*
    四个ByKey算子的区别
    reduceByKey 没有初始值,分区间和分区内的逻辑相同
    foldByKey  有初始值,分区间和分区内的逻辑相同
    aggregateByKey 有初始值,分区间和分区内的逻辑不相同
    combineByKey 有初始值,初始值的类型可以改变,更加灵活
     */

    //关闭sc
    sc.stop()
  }
}
