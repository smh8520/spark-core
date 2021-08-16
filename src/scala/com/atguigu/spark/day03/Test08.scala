package com.atguigu.spark.day03

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author smh
 * @create 2021-06-02 15:50
 */
object Test08 {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List("Hello", "Hello", "Hello", "Hello", "Hello", "Spark", "Spark","Hive","Hive"),2)



    val acc = new MyAccumulator
    sc.register(acc)

    rdd.foreach (word => acc.add(word))

    println(acc)


    //关闭sc
    sc.stop()
  }
}

class MyAccumulator extends AccumulatorV2[String,mutable.Map[String,Int]] {

  private val map: mutable.Map[String, Int] = mutable.Map[String,Int]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = new MyAccumulator

  override def reset(): Unit = map.clear()

  override def add(v: String): Unit = {
    if(v.startsWith("H")){
      map(v)=map.getOrElse(v,0)+1
    }
  }

  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
      val map2 = other.value

    map2.foreach(x=>{
      map(x._1) = map.getOrElse(x._1,0)+x._2
    })
  }

  override def value: mutable.Map[String, Int] = map
}
