package com.atguigu.spark.day05

import com.atguigu.spark.day04.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-06-03 11:41
 */
object Test01 {
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

    val reduceRDD = userRDD.flatMap(x => {
      if (x.click_category_id != "-1") {
        List((x.click_category_id, (1, 0, 0)))
      } else if (x.order_category_ids != "null") {
        val orderIds = x.order_category_ids.split(",")
        orderIds.map(x => (x, (0, 1, 0)))
      } else if (x.pay_category_ids != "null") {
        val payIds = x.pay_category_ids.split(",")
        payIds.map(x => (x, (0, 0, 1)))
      } else {
        Nil
      }
    }).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))

    val tuples: Array[(String, (Int, Int, Int))] = reduceRDD.sortBy(_._2,false).take(10)
    val list: List[String] = tuples.map(_._1).toList

    val resRDD: RDD[(String, Int)] = userRDD.filter(x=>list.contains(x.click_category_id)).map(x=>(x.click_category_id+"_"+x.session_id,1)).reduceByKey(_+_)
    val res: RDD[(String, Iterable[(String, Int)])] = resRDD.map(x => {
      val strings: Array[String] = x._1.split("_")
      (strings(0), (strings(1), x._2))
    }).groupByKey()
    val result: RDD[(String, List[(String, Int)])] = res.mapValues(x=>x.toList.sortWith(_._2>_._2).take(10))

    result.foreach(println)



    //关闭sc
    sc.stop()
  }
}

case class User(date: String, //用户点击行为的日期
                user_id: String, //用户的ID
                session_id: String, //Session的ID
                page_id: String, //某个页面的ID
                action_time: String, //动作的时间点
                search_keyword: String, //用户搜索的关键词
                click_category_id: String, //某一个商品品类的ID
                click_product_id: String, //某一个商品的ID
                order_category_ids: String, //一次订单中所有品类的ID集合
                order_product_ids: String, //一次订单中所有商品的ID集合
                pay_category_ids: String, //一次支付中所有品类的ID集合
                pay_product_ids: String, //一次支付中所有商品的ID集合
                city_id: String)