package com.atguigu.spark.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-05-31 18:18
 */
object Test03 {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Array(1,2,3,4,5,6),3)

    rdd.mapPartitionsWithIndex((x,y)=>y.map((x,_))).foreach(println)

    /*coalesce(n,boolean) 修改分区数 可以将分区数调大,也可以将分区数调小
      boolean的值代表是否走shuffle,默认值为false
      如果是将分区数调大的话,必须为true,走shuffle,不然没有效果
     如果是分区数调小的话,可以走shuffle,也可以不走.走的话重新洗牌分区,不走的话就按照原分区
     start=(分区号*旧分区数)/新分区数
     end=(分区号+1)*旧分区数/新分区数
     */
    rdd.coalesce(4).mapPartitionsWithIndex((x,y)=>y.map((x,_))).foreach(println)

    /*
    repartition(n)的底层调用的是coalesce(n,true)方法,不管是增加分区还是减少分区都要走shuffle
     */
    rdd.repartition(4).mapPartitionsWithIndex((x,y)=>y.map((x,_))).foreach(println)



    //关闭sc
    sc.stop()
  }
}
