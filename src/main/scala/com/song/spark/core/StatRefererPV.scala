package com.song.spark.core

import com.song.spark.dao.StatTabsDao
import com.song.spark.domain.{RefererPVBean, CommonPVCountBean}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer


/**
  * author：song.zhang
  *
  * 项目需求:每个网站的引流情况，并排序
  *
  * desc:离线处理，用sparkcore
  *
  * 日志格式：55.10.167.124	2018-01-23 14:49:29	"GET /inke/shortvideo.html HTTP/1.1"	500	http://www.sogou.com/web?query=游戏
  *
  * 数据100万行
  *
  * 数据写入到HBase中
  */

object StatRefererPV {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("StatRefererPV")

    val sc = new SparkContext(conf)
    //录入数据
    val lines = sc.textFile("/Users/zhangsongdeshendun/data/inke_access.log")
    //数据清洗
    val line = lines.map(line => {
      val infos = line.split("\t")
      var tem = infos(4)
      if (!tem.equals("-")) {
        tem = tem.split("/")(2)
      }
      RefererPVBean(infos(0), infos(1), infos(2), tem)
    }).filter(x => !x.referer.equals("-"))
    //合并同时排序
    val result = line.map(x => (x.referer, 1)).reduceByKey(_ + _).map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1))

    //把结果写入到数据库中
    result.foreachPartition(x => {
      val list = new ListBuffer[CommonPVCountBean]
      x.foreach(y => {
        list.append(CommonPVCountBean(y._1, y._2))
      })
      StatTabsDao.save("stat_browser", list) //表名
    })
    //输出结果
    result.foreach(println)


  }

}
