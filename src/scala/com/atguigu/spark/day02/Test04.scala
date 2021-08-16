package com.atguigu.spark.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-05-31 18:29
 */
object Test04 {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Array(1,2,3,4,5,6,7,8,9,10),2)

    rdd.sample(true,0.5).foreach(println)

    /*
    sample()函数是对数据进行抽样.
    第一个参数为boolean,为true的时候采用的是泊松分布的算法,为false的时候采用的是伯努利分布的算法
    第二个参数为一个double值,在伯努利算法中代表的是每个数选中的概率,在泊松分布的算法中代表的是期望每个数出现的次数
    第三个参数为随机数种子,相同随机数种子产生的随机数结果相同.如果不填的话默认按照当前时间的毫秒数
     */
    //关闭sc
    sc.stop()
  }
}
