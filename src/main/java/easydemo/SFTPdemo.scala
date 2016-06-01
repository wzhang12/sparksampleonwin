package easydemo

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by spark on 2016/5/30.
  */
object SFTPdemo {
  def main(args:Array[String]) {
    val conf = new SparkConf
    conf.setAppName("ftpdemo")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.
      format("com.springml.spark.sftp").
      option("host", "127.0.0.1").
      option("username", "spark").
      option("password", "spark").
      option("fileType", "csv").
      option("inferSchema", "true").
      load("/drives/C/spark/kmeans.csv")
      df.collect().foreach(println)
    sc.stop()
  }
}
