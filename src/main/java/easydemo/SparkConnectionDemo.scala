package easydemo

/**
  * Created by zhangwen on 2016/3/9.
  */
object SparkConnectionDemo {
    def main(args:Array[String]) {
      import org.apache.spark._

      val conf = new SparkConf().setMaster("local[4]").setAppName("NetworkWordCount")
      val sc = new SparkContext(conf)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      val dsl = "jdbc:mysql://localhost:3306/sakila"
      val username = "root"
      val pwd = "tiger"

      val df1 = sqlContext.read.format("jdbc").option("url", dsl)
        .option("driver", "com.mysql.jdbc.Driver")
        .option("dbtable", "address")
        .option("user", username)
        .option("password", pwd)
        .load()
      df1.registerTempTable("addresses")

      val df2 = sqlContext.read.format("jdbc").option("url", dsl)
        .option("driver", "com.mysql.jdbc.Driver")
        .option("dbtable", "city")
        .option("partitionColumn", "city_id")
        .option("lowerBound", "1")
        .option("upperBound", "600")
        .option("numPartitions","2")
        .option("user", username)
        .option("password", pwd)
        .load()
      df2.registerTempTable("cities")

      val sql=sqlContext.sql("select * from addresses join cities on addresses.city_id=cities.city_id ")
      sql.show()


      sc.stop()
    }
}
