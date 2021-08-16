package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-06-01 18:43
 */
object Test04 {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(Array(("a",1),("b",2),("e",5),("c",5)))

    val rdd2 = sc.makeRDD(Array(("a",3),("b",4),("b",4),("e",7),("b",2),("e",5),("a",1)))

    val res: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd2.cogroup(rdd1)

    res.foreach(println)

    //关闭sc
    sc.stop()
  }
}
