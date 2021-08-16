package com.atguigu.spark.day05

import com.atguigu.spark.day04.UserVisitAction
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-06-04 14:47
 */
object Test04 {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("D:\\workspace\\spark-core\\input\\user_visit_action.txt")

    val userRDD = rdd.map(line => {
      val datas: Array[String] = line.split("_")
      //将解析出来的数据封装到样例类里面
      UserVisitAction(
        datas(0),
        datas(1),
        datas(2),
        datas(3),
        datas(4),
        datas(5),
        datas(6),
        datas(7),
        datas(8),
        datas(9),
        datas(10),
        datas(11),
        datas(12)
      )
    })
   val list = List("1","2","3","4","5","6","7")
    val bdList: Broadcast[List[String]] = sc.broadcast(list)
    val lists: List[String] = list.zip(list.tail).map(x=>x._1+"-"+x._2)
    val bdLists: Broadcast[List[String]] = sc.broadcast(lists)

    val fmMap: Map[String, Int] = userRDD.filter(x => bdList.value.init.contains(x.page_id))
      .groupBy(_.page_id).map(x => (x._1, x._2.toList.size)).collect().toMap

    val sortRDD: RDD[(String, List[UserVisitAction])] = userRDD.groupBy(_.session_id).mapValues(_.toList.sortBy(_.action_time))
    val pagesRDD: RDD[String] = sortRDD.mapValues(x => {
      val pages: List[String] = x.map(_.page_id)
      val page2page: List[(String, String)] = pages.zip(pages.tail)
      page2page.map(x => x._1 + "-" + x._2).filter(x => bdLists.value.contains(x))

    }).map(_._2).flatMap(x => x)
    val pageCountRDD: RDD[(String, Int)] = pagesRDD.map((_,1)).reduceByKey(_+_)

    pageCountRDD.map{
      case (pages,cnt)=>{
        val i: Int = fmMap(pages.charAt(0).toString)
        (pages,cnt.toDouble/i)
      }
    }.foreach(println)







    //关闭sc
    sc.stop()
  }
}
