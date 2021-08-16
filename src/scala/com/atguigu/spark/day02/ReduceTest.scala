package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-05-31 19:43
 */
object ReduceTest {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)
//    val rdd = sc.makeRDD(List(("a",1),("b",5),("a",5),("b",2)))
//
//    rdd.reduceByKey(_+_).collect().foreach(println)

//    val rdd = sc.makeRDD(List(("a",1),("a",3),("a",5),("b",7),("b",2),("b",4),("b",6),("a",7)), 2)
//    rdd.aggregateByKey(10)(_+_,_+_).collect().foreach(println)
//val rdd = sc.makeRDD(List(("a",1),("a",3),("a",5),("b",7),("b",2),("b",4),("b",6),("a",7)), 2)
//    rdd.foldByKey(10)(_+_).collect().foreach(println)

    val list: List[(String, Int)] = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
    val rdd: RDD[(String, Int)] = sc.makeRDD(list, 2)

    rdd.combineByKey((_,1),(x:(Int,Int),y)=>(x._1+y,x._2+1),(x:(Int,Int),y:(Int,Int))=>(x._1+y._1,x._2+y._2)).collect().foreach(println)

    //关闭sc
    sc.stop()
  }
}
