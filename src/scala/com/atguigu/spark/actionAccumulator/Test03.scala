package com.atguigu.spark.actionAccumulator

import com.atguigu.spark.day04.UserVisitAction
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-06-04 19:02
 */
object Test03 {
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

    val l = List("1","2","3","4","5","6","7")
    val bdList: Broadcast[List[String]] = sc.broadcast(l)
    val res: List[String] = l.zip(l.tail).map(x=>x._1+"_"+x._2)
    val bdLists: Broadcast[List[String]] = sc.broadcast(res)

    val fmMap: Map[String, Int] = actionRDD.filter(x=>bdList.value.contains(x.page_id)).map(x=>(x.page_id,1)).reduceByKey(_+_).collect().toMap

    val sessRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(_.session_id)
    val pageCount: RDD[(String, Int)] = sessRDD.mapValues(x => {
      val pages: List[String] = x.toList.sortBy(x => x.action_time).map(_.page_id)

      val page2page: List[String] = pages.zip(pages.tail).map(x => x._1 + "_" + x._2)
      page2page.filter(x => bdLists.value.contains(x))

    }).flatMap(_._2).map((_, 1)).reduceByKey(_ + _)

    val result: RDD[(String, Double)] = pageCount.map {
      case (p, c) => {
        val strings: Array[String] = p.split("_")
        val double: Double = fmMap.getOrElse(strings(0), 0).toDouble
        (p, c / double)
      }
    }
    result.foreach(println)








    //关闭sc
    sc.stop()
  }
}
