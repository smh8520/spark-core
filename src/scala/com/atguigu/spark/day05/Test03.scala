package com.atguigu.spark.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author smh
 * @create 2021-06-04 8:52
 */
object Test03 {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)
  val ac = new TestAccumulator
    sc.register(ac)
    val rdd: RDD[String] = sc.makeRDD( List("Hello", "Hello", "Hello", "Hello", "Spark", "Spark"))

    rdd.foreach(x=>ac.add(x))

    println(ac.value)

    //关闭sc
    sc.stop()
  }
}

class TestAccumulator extends AccumulatorV2[String,mutable.Map[String,Int]]{
  private val map: mutable.Map[String, Int] = mutable.Map[String,Int]()
  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = new TestAccumulator

  override def reset(): Unit = map.clear()

  override def add(v: String): Unit = {
    if(v.startsWith("H")){
      map(v)=map.getOrElse(v,0)+1
    }
  }

  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    other.value.foreach{
      case (word,cnt)=>{
        map(word) = map.getOrElse(word,0)+cnt
      }
    }
  }

  override def value: mutable.Map[String, Int] = map
}