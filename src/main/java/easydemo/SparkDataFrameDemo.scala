package easydemo

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by zhangwen on 2016/3/8.
  */

case class User(id:Int,name:String,depId:Int)
case class Department(id:Int,name:String)

object SparkDataFrameDemo {
  def main(args:Array[String]){
    val departmentRecords = "D:\\测试文本\\department.txt"
    val userRecords = "D:\\测试文本\\user.txt"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // 导入语句，可以隐式地将RDD转化成DataFrame
    import sqlContext.implicits._



    val dfDepartment = sc.textFile(departmentRecords).map(_.split(",")).map(p => Department(p(0).trim.toInt, p(1))).toDF()
    val dfUser = sc.textFile(userRecords).map(_.split(",")).map(p => User(p(0).trim.toInt, p(1),p(2).trim.toInt)).toDF()

    dfDepartment.registerTempTable("department")
    dfUser.registerTempTable("user")

    dfDepartment.show()
    dfUser.show()

    val aggTable=dfUser.join(dfDepartment,dfDepartment("id")===dfUser("depId")).groupBy(dfDepartment("id"),dfDepartment("name")).count()
    aggTable.filter(aggTable("count")>4).show()

    //============================================
    //sql
    //============================================
    sqlContext.sql("select department.id ,department.name ,count(*) as count from department join user on user.depId=department.id group by department.id, department.name  having count>4 ").show()

    sc.stop();
  }
}
