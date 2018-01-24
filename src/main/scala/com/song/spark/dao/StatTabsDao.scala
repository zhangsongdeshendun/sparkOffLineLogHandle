package com.song.spark.dao

import com.song.spark.domain.CommonPVCountBean
import com.song.spark.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.mutable.ListBuffer

object StatTabsDao {

  val tableName = "stat_tab"
  //表名
  val cf = "info"
  //列簇名
  val qualifer = "click_count" //列名

  /**
    * 保存数据到hbase
    *
    * @param list CourseClickCount的集合
    */
  def save(tableName: String, list: ListBuffer[CommonPVCountBean]): Unit = {

    val table = HBaseUtils.getInstance().getTable(tableName)

    for (ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.rowKey),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifer),
        ele.count)
    }

  }

  /**
    * 根据rowkey查询值
    *
    * @param day_course
    * @return
    */
  def count(day_course: String): Long = {
    val table = HBaseUtils.getInstance().getTable(tableName)

    val get = new Get(Bytes.toBytes(day_course))
    val value = table.get(get).getValue(cf.getBytes, qualifer.getBytes)
    if (value == 0) {
      0l
    } else {
      Bytes.toLong(value)
    }

  }

  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[CommonPVCountBean]
    list.append(CommonPVCountBean("20170112_8", 8))
    list.append(CommonPVCountBean("20170112_9", 9))
    list.append(CommonPVCountBean("20170112_1", 100))

    save("stat_tab", list)

    print(count("20170112_8") + " " + count("20170112_9") + "   " + count("20170112_1"))
  }

}
