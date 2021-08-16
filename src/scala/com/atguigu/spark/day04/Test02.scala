package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{immutable, mutable}

/**
 * @author smh
 * @create 2021-06-02 18:51
 */
object Test02 {
  def main(args: Array[String]): Unit = {
//    //创建Spark配置文件
//    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
//    //创建Spark上下文对象
//    val sc = new SparkContext(conf)
//    val rdd = sc.textFile("D:\\workspace\\spark-core\\input\\user_visit_action.txt")
//
//    val acc = new CategoryCountAccumulator
//    sc.register(acc)
//
//    val userRDD = rdd.map(line => {
//      val datas: Array[String] = line.split("_")
//      //将解析出来的数据封装到样例类里面
//      UserVisitAction(
//        datas(0),
//        datas(1),
//        datas(2),
//        datas(3),
//        datas(4),
//        datas(5),
//        datas(6),
//        datas(7),
//        datas(8),
//        datas(9),
//        datas(10),
//        datas(11),
//        datas(12)
//      )
//    })
//    userRDD.foreach(x=>acc.add(x))
//
//    val groupMap: Map[String, mutable.Map[(String, String), Int]] = acc.value.groupBy(_._1._1)
//
//    val list = groupMap.map {
//      case (id, map) => {
//        val click = map.getOrElse((id, "click"), 0)
//        val order = map.getOrElse((id, "order"), 0)
//        val pay = map.getOrElse((id, "pay"), 0)
//        CategoryCountInfo(id, click, order, pay)
//      }
//    }
//    val info = list.toList.sortBy(x=>(x.clickCount,x.orderCount,x.payCount))(Ordering[(Long,Long,Long)].reverse).take(10)
//
//    val idList= info.map(_.categoryId)
//
//    val bcl = sc.broadcast(idList)
//
//    val filterRDD = userRDD.filter(user => {
//      if (user.click_category_id != "-1") {
//        bcl.value.contains(user.click_category_id)
//      }else{
//        false
//      }
//
//    })
//    val sessionRDD = filterRDD.map(user => {
//      (user.click_category_id + "_" + user.session_id, 1)
//    })
//    val sessionCountRDD: RDD[(String, Int)] = sessionRDD.reduceByKey(_+_)
//
//    val res: RDD[(String, Iterable[(String, Int)])] = sessionCountRDD.map(x => {
//      val strings = x._1.split("_")
//      (strings(0), (strings(1), x._2))
//    }).groupByKey()
//
//    res.mapValues(x=>x.toList.sortWith(_._2>_._2).take(10)).collect().foreach(println)
//
//
//    //关闭sc
//    sc.stop()
  }
}
//class CategoryCountAccumulator extends AccumulatorV2[UserVisitAction,mutable.Map[(String,String),Int]]{
//  private val map: mutable.Map[(String, String), Int] = mutable.Map[(String,String),Int]()
//  override def isZero: Boolean = map.isEmpty
//
//  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Int]] = new CategoryCountAccumulator
//
//  override def reset(): Unit = map.clear()
//
//  override def add(v: UserVisitAction): Unit = {
//    if(v.click_category_id != "-1"){
//
//      var key = (v.click_category_id,"click")
//        map(key)=map.getOrElse(key,0)+1
//
//    }else if(v.order_category_ids != "null"){
//      val strings = v.order_category_ids.split(",")
//
//      strings.foreach(x=>{
//        var key=(x,"order")
//        map(key)=map.getOrElse(key,0)+1
//      })
//
//    }else if(v.pay_category_ids != "null"){
//      val str: Array[String] = v.pay_category_ids.split(",")
//      for(s<- str){
//        var key=(s,"pay")
//        map(key)=map.getOrElse(key,0)+1
//      }
//
//    }else{
//      Nil
//    }
//  }
//
//  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Int]]): Unit ={
//        val map2 = other.value
//        map2.foreach{
//          case (key,value)=>{
//            map(key)=map.getOrElse(key,0)+value
//          }
//        }
//
//  }
//
//  override def value: mutable.Map[(String, String), Int] = map
//}

//case class UserVisitAction(date: String,//用户点击行为的日期
//                           user_id: String,//用户的ID
//                           session_id: String,//Session的ID
//                           page_id: String,//某个页面的ID
//                           action_time: String,//动作的时间点
//                           search_keyword: String,//用户搜索的关键词
//                           click_category_id: String,//某一个商品品类的ID
//                           click_product_id: String,//某一个商品的ID
//                           order_category_ids: String,//一次订单中所有品类的ID集合
//                           order_product_ids: String,//一次订单中所有商品的ID集合
//                           pay_category_ids: String,//一次支付中所有品类的ID集合
//                           pay_product_ids: String,//一次支付中所有商品的ID集合
//                           city_id: String)

//case class CategoryCountInfo(var categoryId: String,//品类id
//                             var clickCount: Long,//点击次数
//                             var orderCount: Long,//订单次数
//                             var payCount: Long)//支付次数