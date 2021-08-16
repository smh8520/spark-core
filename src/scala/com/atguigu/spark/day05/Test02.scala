package com.atguigu.spark.day05

import com.atguigu.spark.day04.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{immutable, mutable}

/**
 * @author smh
 * @create 2021-06-03 14:27
 */
object Test02 {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("D:\\workspace\\spark-core\\input\\user_visit_action.txt")

    val userRDD = rdd.map(line => {
      val datas: Array[String] = line.split("_")
      //将解析出来的数据封装到样例类里面
      Users(
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
    val accumulator = new UserAccumulator
    sc.register(accumulator)
    userRDD.foreach(x=>accumulator.add(x))

    val map: Map[String, mutable.Map[(String, String), Int]] = accumulator.value.groupBy(_._1._1)

    val tuples: Map[String, (Int, Int, Int)] =map.map(x=>(x._1,(x._2.getOrElse((x._1,"click"),0),x._2.getOrElse((x._1,"order"),0),x._2.getOrElse((x._1,"pay"),0))))

    val res: List[(String, (Int, Int, Int))] = tuples.toList.sortBy(_._2)(Ordering[(Int,Int,Int)].reverse).take(10)

    res.foreach(println)


    //关闭sc
    sc.stop()
  }
}
//累加器
class UserAccumulator extends AccumulatorV2[Users,mutable.Map[(String,String),Int]]{
  private var map: mutable.Map[(String, String), Int] = mutable.Map[(String,String),Int]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[Users, mutable.Map[(String, String), Int]] = new UserAccumulator

  override def reset(): Unit = map.clear()
//累加
  override def add(v: Users): Unit = {
    if(v.click_category_id != "-1"){
      val key = (v.click_category_id,"click")
      map(key)=map.getOrElse(key,0)+1
    }else if(v.order_category_ids != "null"){
      val order: Array[String] = v.order_category_ids.split(",")
      for(o<- order){
        val key = (o,"order")
        map(key)=map.getOrElse(key,0)+1
      }
    }else if(v.pay_category_ids != "null"){
      val pay: Array[String] = v.pay_category_ids.split(",")
      for(p<- pay){
        val key = (p,"pay")
        map(key)=map.getOrElse(key,0)+1
      }
    }else{
      Nil
    }

  }

  override def merge(other: AccumulatorV2[Users, mutable.Map[(String, String), Int]]): Unit = {
    other.value.foreach{
      case (k,v)=>{
        map(k)=map.getOrElse(k,0)+v
      }
    }
  }

  override def value: mutable.Map[(String, String), Int] = map
}

case class Users(date: String, //用户点击行为的日期
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