package com.atguigu.spark.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * value-value类型的RDD算子
 * @author smh
 * @create 2021-05-31 19:03
 */
object Test07 {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(1 to 4)
    val rdd2 = sc.makeRDD(3 to 6)

    //intersection() 求两个RDD的交集
    println(rdd1.intersection(rdd2).collect().mkString(","))

    //union() 求两个RDD的并集,不去重
    println(rdd1.union(rdd2).collect().mkString(","))

    //subtract() 求两个RDD的差集
    println(rdd1.subtract(rdd2).collect().mkString(","))//1 2
    println(rdd2.subtract(rdd1).collect().mkString(","))//5

    //zip() 拉链,返回值是一个二元组,二元组的第一位为调用者的元素,第二位为参数的元素,要求两个RDD的分区数以及每个分区内的元素个数必须相等
    rdd1.zip(rdd2).mapPartitionsWithIndex((x,y)=>y.map((x,_))).foreach(println)

    //关闭sc
    sc.stop()
  }
}
