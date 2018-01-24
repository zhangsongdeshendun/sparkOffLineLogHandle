package com.song.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 一个简单的wordcount ，测试本地环境
  */

object WordCount {

  def main(args: Array[String]): Unit = {

    val conf=new SparkConf().setMaster("local[2]").setAppName("wordcount")
    val sc=new SparkContext(conf);

    val lines=sc.textFile("/Users/zhangsongdeshendun/data/wordcount.txt")

    val result=lines.flatMap(line=>line.split(" ")).map(word=>(word,1)).reduceByKey(_+_)

    result.foreach(println)

  }

}
