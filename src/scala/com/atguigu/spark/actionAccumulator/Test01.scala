package com.atguigu.spark.actionAccumulator

import com.atguigu.spark.day04.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-06-04 18:04
 */
object Test01 {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("D:\\workspace\\spark-core\\input\\user_visit_action.txt")
    val actionRDD: RDD[UserVisitAction] = rdd.map(line => {
      val items: Array[String] = line.split("_")
      UserVisitAction(
        items(0),
        items(1),
        items(2),
        items(3),
        items(4),
        items(5),
        items(6),
        items(7),
        items(8),
        items(9),
        items(10),
        items(11),
        items(12)
      )
    })
    val res: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(x => {
      if (x.click_category_id != "-1") {
        List((x.click_category_id, (1, 0, 0)))
      } else if (x.order_category_ids != "null") {
        val cIds: Array[String] = x.order_category_ids.split(",")
        cIds.map((_, (0, 1, 0)))
      } else if (x.pay_category_ids != "null") {
        val cIds: Array[String] = x.pay_category_ids.split(",")
        cIds.map((_, (0, 0, 1)))
      } else Nil
    }).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))
    res.sortBy(_._2,false).take(10).foreach(println)
    //关闭sc
    sc.stop()
  }
}
