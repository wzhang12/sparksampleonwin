package easydemo

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by spark on 2016/5/30.
  */
object FTPdemo {
  def main(args:Array[String]) {
    val conf = new SparkConf
    conf.setAppName("ftpdemo")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)
//    val file = sc.textFile("ftp://1:1@127.0.0.1/kmeans.csv")
//    file.foreach(println)

   // val  file = sc.textFile("C:\\spark\\wildcard\\hr=*")
    val  file = sc.textFile("C:\\spark\\wildcard\\[0-9]*.txt")
    file.collect().foreach(println)

    //

    sc.stop()
  }
}
