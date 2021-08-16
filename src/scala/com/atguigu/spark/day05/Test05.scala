package com.atguigu.spark.day05

import com.atguigu.spark.day04.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-06-04 16:43
 */
object Test05 {
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
    val fmMap: Map[String, Int] = userRDD.map(x=>(x.page_id,1)).reduceByKey(_+_).collect().toMap


    val pagesRDD: RDD[(String, List[String])] = userRDD.groupBy(_.session_id).mapValues(x=>x.toList.sortBy(_.action_time)).mapValues(_.map(_.page_id))

    val page2pageRDD: RDD[(String, Int)] = pagesRDD.mapValues(x => {
      x.zip(x.tail).map(y => y._1 + "-" + y._2)
    }).map(_._2).flatMap(x => x).map((_, 1)).reduceByKey(_ + _)

    page2pageRDD.map{
      case (pages,cnt)=>{
        val p: Array[String] = pages.split("-")
        val i: Int = fmMap(p(0))
        (p(0),(pages,cnt.toDouble/i))
      }
    }.groupByKey().foreach(println)





    //关闭sc
    sc.stop()
  }
}
