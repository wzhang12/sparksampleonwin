package easydemo


import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.phoenix.spark._

/**
  * Created by spark on 2016/5/30.
  */
object HbaseDemo {
  def main(args:Array[String]) {


    val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("NetworkWordCount"))
    val sqlContext = new SQLContext(sc)

    // Load the columns 'ID' and 'COL1' from TABLE1 as a DataFrame
    val df = sqlContext.read.format("org.apache.phoenix.spark").option("table","test7").option("zkUrl","192.168.86.139:2181").load();

    val df1 = sqlContext.read.format("jdbc").option("url", "jdbc:phoenix:192.168.86.139:2181")
      .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
      .option("dbtable", "test7")
      .load()
    df1.registerTempTable("test7");

    // df1.write.mode("overwrite").format("org.apache.phoenix.spark").option("table", "test6").option("zkUrl", "192.168.86.139:2181").save()//append
    //df1.saveToPhoenix("test6",zkUrl = Some("192.168.86.139:2181")) 是append
    sc.stop()
  }
}
