package com.atguigu.spark.day04

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{immutable, mutable}

/**
 * @author smh
 * @create 2021-06-02 20:31
 */
object Test03 {
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
    val acc = new MyAccumulator
    sc.register(acc)

    userRDD.foreach(x=>acc.add(x))

     val map: Map[String, mutable.Map[(String, String), Long]] = acc.value.groupBy(_._1._1)

    val res: Map[String, (Long, Long, Long)] = map.map(x => {
      val click = x._2.getOrElse((x._1, "click"), 0L)
      val order = x._2.getOrElse((x._1, "order"), 0L)
      val pay = x._2.getOrElse((x._1, "pay"), 0L)
      (x._1, (click, order, pay))
    })
    res.toList.sortBy(_._2)(Ordering[(Long,Long,Long)].reverse).take(10).foreach(println)




    //关闭sc
    sc.stop()
  }
}
case class UserVisitAction(date: String,//用户点击行为的日期
                           user_id: String,//用户的ID
                           session_id: String,//Session的ID
                           page_id: String,//某个页面的ID
                           action_time: String,//动作的时间点
                           search_keyword: String,//用户搜索的关键词
                           click_category_id: String,//某一个商品品类的ID
                           click_product_id: String,//某一个商品的ID
                           order_category_ids: String,//一次订单中所有品类的ID集合
                           order_product_ids: String,//一次订单中所有商品的ID集合
                           pay_category_ids: String,//一次支付中所有品类的ID集合
                           pay_product_ids: String,//一次支付中所有商品的ID集合
                           city_id: String)
class MyAccumulator extends AccumulatorV2[UserVisitAction,mutable.Map[(String,String),Long]]{

  private val map: mutable.Map[(String, String), Long] = mutable.Map[(String,String),Long]()

  override def isZero: Boolean =map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = new MyAccumulator

  override def reset(): Unit = map.clear()

  override def add(action: UserVisitAction): Unit = {
    if(action.click_category_id != "-1"){
      //点击  key=(cid,"click")
      val key = (action.click_category_id,"click")
      map(key) = map.getOrElse(key,0L) + 1L
    }else if(action.order_category_ids != "null"){
      //下单  key=(cid,"order")
      val cids: Array[String] = action.order_category_ids.split(",")
      for (cid <- cids) {
        val key = (cid,"order")
        map(key) = map.getOrElse(key,0L) + 1L
      }

    }else if(action.pay_category_ids != "null"){
      //支付  key=(cid,"pay")
      val cids: Array[String] = action.pay_category_ids.split(",")
      for (cid <- cids) {
        val key = (cid,"pay")
        map(key) = map.getOrElse(key,0L) + 1L
      }
    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit ={
    other.value.foreach {
      case (key, count) => {
        map(key) = map.getOrElse(key, 0L) + count
      }
    }

  }

  override def value: mutable.Map[(String, String), Long] = map
}
