package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-06-02 18:05
 */
object Test01 {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("D:\\workspace\\spark-core\\input\\user_visit_action.txt")

    val splitRdd = rdd.map(_.split("_"))
    //过滤处点击,订单,支付的数据
    val clickRDD = splitRdd.filter(_(6).toLong != -1)
    val orderRDD = splitRdd.filter(_(8)!="null")
    val payRDD = splitRdd.filter(_(10)!="null")
    //映射出点击,订单,支付信息
    val clickIdRDD = clickRDD.map(x=>(x(6).toLong,1)) //(id,1)
    val orderIdsRDD = orderRDD.map(_(8)).flatMap(_.split(",").map(x=>(x.toLong,1)))//(id,1)
    val payIdsRDD = payRDD.map(_(10)).flatMap(_.split(",").map(x=>(x.toLong,1))) //(id,1)

    val clickCountRDD = clickIdRDD.reduceByKey(_+_)
    val orderCountRDD = orderIdsRDD.reduceByKey(_+_)
    val payCountRDD = payIdsRDD.reduceByKey(_+_)

    val resRDD: RDD[(Long, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickCountRDD.cogroup(orderCountRDD,payCountRDD)

    val resultRDD: RDD[(Long, (Long, Long, Long))] = resRDD.mapValues(x => {
      val sum1 = x._1.toList.sum.toLong
      val sum2 = x._2.toList.sum.toLong
      val sum3 = x._3.toList.sum.toLong
      (sum1,sum2,sum3)
    })
    resultRDD.sortBy(_._2,false).take(10).foreach(println)



    //关闭sc
    sc.stop()
  }
}
