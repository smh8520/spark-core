package com.atguigu.spark.actionAccumulator

import com.atguigu.spark.day04.UserVisitAction
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author smh
 * @create 2021-06-04 18:24
 */
object ActionAccumulate {
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
    val actionAcc = new ActionAccumulator
    sc.register(actionAcc)
    actionRDD.foreach(x=>actionAcc.add(x))

    val map: Map[String, mutable.Map[(String, String), Int]] = actionAcc.value.groupBy(_._1._1)
    val result: Map[String, (Int, Int, Int)] = map.map(x => {
      val click: Int = x._2.getOrElse((x._1, "click"), 0)
      val order: Int = x._2.getOrElse((x._1, "order"), 0)
      val pay: Int = x._2.getOrElse((x._1, "pay"), 0)
      (x._1, (click, order, pay))
    })
    val Ids: List[String] = result.toList.sortBy(_._2)(Ordering[(Int,Int,Int)].reverse).take(10).map(_._1)
    val IdList: Broadcast[List[String]] = sc.broadcast(Ids)
    val sessRDD: RDD[(String, Int)] = actionRDD.filter(x => IdList.value.contains(x.click_category_id))
      .map(x => {
        (x.click_category_id+"="+x.session_id,1)
      }).reduceByKey(_+_)
    val sessResult: RDD[(String, List[(String, Int)])] = sessRDD.map(x => {
      val s: Array[String] = x._1.split("=")
      (s(0), (s(1), x._2))
    }).groupByKey().mapValues(x => x.toList.sortBy(x => x._2)(Ordering[Int].reverse).take(10))
    sessResult.foreach(println)

    //关闭sc
    sc.stop()
  }
}

class ActionAccumulator extends AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Int]] {
  private val map: mutable.Map[(String, String), Int] = mutable.Map[(String, String), Int]()
  //初始化累加器
  override def isZero: Boolean = map.isEmpty
  //复制累加器
  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Int]] = new ActionAccumulator
  //重置累加器
  override def reset(): Unit = map.clear()
  //累加操作
  override def add(v: UserVisitAction): Unit = {
    if (v.click_category_id != "-1") {
      val key = (v.click_category_id, "click")
      map(key) = map.getOrElse(key, 0) + 1
    } else if (v.order_product_ids != "null") {
      val cIds: Array[String] = v.order_product_ids.split(",")
      cIds.foreach {
        case s => {
          var key = (s, "order")
          map(key) = map.getOrElse(key, 0) + 1
        }
      }
    } else if (v.pay_category_ids != "null") {
      val cIds: Array[String] = v.pay_category_ids.split(",")
      cIds.foreach {
        case s => {
          var key = (s, "pay")
          map(key) = map.getOrElse(key, 0) + 1
        }
      }
    } else Nil

  }
  //累加器合并
  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Int]]): Unit = {
    other.value.foreach{
      case (key,value) =>{
        map(key)=map.getOrElse(key,0)+value
      }
    }
  }

  override def value: mutable.Map[(String, String), Int] = map
}