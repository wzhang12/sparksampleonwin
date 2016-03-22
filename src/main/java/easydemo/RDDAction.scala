package easydemo

/**
  * Created by zhangwen on 2016/3/8.
  */
import org.apache.spark._


object RDDAction {
  def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
      val sc = new SparkContext(conf)



    sc.stop()
  }




}
