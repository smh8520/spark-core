package com.atguigu.spark.actionAccumulator

import com.atguigu.spark.day04.UserVisitAction
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-06-04 19:53
 */
object Test04 {
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

    val map: Map[String, Int] = actionRDD.map(x=>(x.page_id,1)).reduceByKey(_+_).collect().toMap
    val bd: Broadcast[Map[String, Int]] = sc.broadcast(map)

    val sess: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(_.session_id)

    val page2PageCount: RDD[(String, Int)] = sess.mapValues(x => {
      val pages: List[String] = x.toList.sortBy(x => x.action_time).map(_.page_id)
      pages.zip(pages.tail).map(x => x._1 + "-" + x._2)
    }).flatMap(x => x._2.map((_, 1))).reduceByKey(_ + _)

    val result
    = page2PageCount.map {
      case (k, v) => {
        val strings: Array[String] = k.split("-")
        val double: Double = bd.value.getOrElse(strings(0), 0).toDouble
        (strings(0), (k, v / double))
      }
    }.groupByKey()
    result.foreach(println)

    //关闭sc
    sc.stop()
  }
}
