package fr.openvalue

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCount {
  def main(args : Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("WordCount en Spark Scala"))

    val os = "linux"
    //val os = "windows"
    // val treshold = args(1).toInt
    val treshold = 10

    val text = if (os == "windows") "file://C:\\Users\\nicol\\Desktop" else "hdfs:///user/root/ainventor-short.csv"
    //val text = args(0)

    var words = sc.textFile(text).flatMap(_.split(" "))

    // count the occurrence of each wordl
    var wordCounts = words.map((_, 1)).reduceByKey(_ + _)

    // filter out words with less than threshold occurrences
    var filtered = wordCounts.filter(_._2 >= treshold)

    // count characters
    var letterCounts = filtered.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _)

    println(letterCounts.collect().mkString(", "))
  }
}