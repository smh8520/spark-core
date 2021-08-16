package com.atguigu.spark.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-06-01 18:34
 */
object Test03 {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer").registerKryoClasses(Array(classOf[User]))
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val zs = new User("zs",15)
    val ls = new User("ls",18)

    val rdd = sc.makeRDD(Array(zs,ls))



    rdd.foreach(x=>println(x))

    //关闭sc
    sc.stop()
  }
}

case class User(val name:String,var age:Int){



}