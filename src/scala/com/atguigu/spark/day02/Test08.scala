package com.atguigu.spark.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-05-31 19:11
 */
object Test08 {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(("hello world",4),("hello spark",3),("world hadoop",1),("spark",1)))

//    //利用groupBy求WordCount
//    rdd.flatMap(_.split(" ")).groupBy(_+"").map{
//      case (k,v)=>(k,v.size)
//    }.collect().foreach(println)

//    使用groupByKey求WordCount
//    rdd.flatMap(_.split(" ").map((_,1))).groupByKey().map(x=>(x._1,x._2.sum)).collect().foreach(println)
    rdd.flatMap(x=>x._1.split(" ").map((_,x._2))).groupByKey().map{
      case (x,y)=>(x,y.sum)
    }.collect().foreach(println)



    //groupBy和groupByKey的区别
    /*
    groupBy算子只要是RDD都可以调用.它会根据逻辑对数据进行分组.
    如果是对数据是键值对的集合分组按照key来分组的话,则分组后,key为原key,值为一个个二元组

    groupByKey算子只有数据是k,v的RDD才可以调用,它会按照k,v的k来对键值对进行分组
    key是原key,值是每个相同k的k,v的value
     */

    //关闭sc
    sc.stop()
  }
}
