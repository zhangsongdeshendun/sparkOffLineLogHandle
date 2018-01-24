package com.song.spark.core

import com.song.spark.dao.StatTabsDao
import com.song.spark.domain.{TabsPVBean, CommonPVCountBean}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer

/**
  * author：song.zhang
  *
  * 项目需求:统计热门，短视频，游戏，发现，音频五个tab的日访问量，并排序
  *
  * desc:离线处理，用sparkcore
  *
  * 日志格式：55.10.167.124	2018-01-23 14:49:29	"GET /inke/shortvideo.html HTTP/1.1"	500	http://www.sogou.com/web?query=游戏
  *
  * 数据100万行
  *
  * 数据写入到HBase中
  */

object StatTabsPV {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("StatTabsPV")

    val sc = new SparkContext(conf)
    //录入数据
    val lines = sc.textFile("/Users/zhangsongdeshendun/data/inke_access.log")
    //数据清洗
    val line = lines.map(line => {
      val infos = line.split("\t")
      val url = infos(2).split(" ")(1)
      val tem = url.split("/")(2)
      val tab = tem.substring(0, tem.lastIndexOf("."))
      TabsPVBean(infos(0), infos(1), tab, infos(4))
    }).filter(x => (!x.tab.equals("h5")) && (!x.tab.equals("web")))
    //合并同时排序
    val result = line.map(x => (x.tab, 1)).reduceByKey(_ + _).map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1))
    //把结果写入到数据库中
    result.foreachPartition(x => {
      val list = new ListBuffer[CommonPVCountBean]
      x.foreach(y => {
        list.append(CommonPVCountBean(y._1, y._2))
      })
      StatTabsDao.save("stat_tab", list) //表名
    })

    //输出结果
    result.foreach(println)


  }


}
