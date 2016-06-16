package easydemo

import java.util.Properties

import org.apache.spark.sql.SaveMode
import org.apache.hadoop.conf.Configuration
/**
  * Created by zhangwen on 2016/3/9.
  */
object SparkConnectionDemo {
    def main(args:Array[String]) {
      import org.apache.spark._
      val conf = new SparkConf().setMaster("local[4]").setAppName("NetworkWordCount")
      val sc = new SparkContext(conf)
      sc.hadoopConfiguration.setBoolean("dfs.support.append", true)
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

     val sql=sqlContext.sql("select address_id,address,district,postal_code,phone,location,city,country_id from addresses left join cities on addresses.city_id=cities.city_id ")
     sql.show()

      import org.apache.spark.sql.functions.udf
      val toUpperFn: String => String = _.toUpperCase
      val toUpperUDF = udf(toUpperFn)
      sqlContext.udf.register("toUpperFn",toUpperFn)

//      val sql = sqlContext.sql("select toUpperFn(district) from addresses")
//      sql.show()

      val groupConcat = new GroupConcat(",")
      sqlContext.udf.register("groupConcat",groupConcat)
/*      val sql = sqlContext.sql("select groupConcat(city_id) from addresses")
      sql.collect().foreach(println)*/

      val prop = new Properties()
      prop.put("user", "root")
      prop.put("password", "tiger")

      // sql.write.mode("append").jdbc("jdbc:mysql://127.0.0.1:3306/sakila", "district", prop)
      //sql.write.format("com.databricks.spark.csv").save("hdfs://192.168.86.139:9000/testsavahdfs1")//这个格式不能增量不能append
      //结果[415,432 Garden Grove Street,,Ontario,430,65630,615964523510,[B@4fd6d219,2014-09-25 22:30:28.0,430,Richmond Hill,20,2006-02-15 04:45:25.0]
      //sqlContext.read.format("com.databricks.spark.csv").text("hdfs://192.168.86.139:9000/testsavahdfs1").collect().foreach(println)
      //sqlContext.read.format("com.databricks.spark.csv").load("hdfs://192.168.86.139:9000/testsavahdfs1").collect().foreach(println)

/*       //sql.write.save("hdfs://192.168.86.139:9000/testsavahdfs2")//spark默认用parquet存
      //结果[415,432 Garden Grove Street,Ontario,65630,615964523510,[B@236b4a44,Richmond Hill,20]
      sqlContext.read.parquet("hdfs://192.168.86.139:9000/testsavahdfs2").collect().foreach(println)//用parquet取*/
      //sql.write.mode("append").save("hdfs://192.168.86.139:9000/testsavahdfs2")支持append


      //sql.rdd.saveAsTextFile("hdfs://192.168.86.139:9000/testsavahdfs3")
      //结果[[413,692 Amroha Drive,Northern,35575,359478883004,[B@6d279a53,Jaffna,88]]
      //sqlContext.read.text("hdfs://192.168.86.139:9000/testsavahdfs3").collect().foreach(println)
      //报错 Text data source supports only a single column, and you have 8 columns.;
      // sql.write.mode("append").text("hdfs://192.168.86.139:9000/testsavahdfs3")
      //Text data source supports only a single column, and you have 8 columns
      //sql.write.text("hdfs://192.168.86.139:9000/testsavahdfs3")

      //sql.write.saveAsTable("testsavahdfs4")//需要集成hive



      import sqlContext.implicits._
//      val userRecords = "C:\\测试文本\\user.txt"
//      val dfUser = sc.textFile(userRecords).map(_.split(",")).map(p => User(p(0).trim.toInt, p(1),p(2).trim.toInt)).toDF()
//      dfUser.registerTempTable("user")
//      val df3 = sqlContext.read.format("jdbc").option("url", dsl)
//        .option("driver", "com.mysql.jdbc.Driver")
//        .option("dbtable", "department")
//        .option("user", username)
//        .option("password", pwd)
//        .load()
//      df3.registerTempTable("department")
//      val sqlDiff = sqlContext.sql("select * from user join department on department.ids=user.depId")
//      sqlDiff.write.jdbc("jdbc:mysql://127.0.0.1:3306/sakila", "departmentuser", prop)
//      sqlDiff.show()

      sc.stop()
    }
}
