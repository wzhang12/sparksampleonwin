package easydemo

/**
  * Created by zhangwen on 2016/3/9.
  */
object SparkStreamingDemo {
    def main(args:Array[String]){
      import org.apache.spark._
      import org.apache.spark.streaming._
      import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3

      // Create a local StreamingContext with two working thread and batch interval of 1 second.
      // The master requires 2 cores to prevent from a starvation scenario.

      val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
      val ssc = new StreamingContext(conf, Seconds(1))
      val lines = ssc.socketTextStream("192.168.137.101", 9998)
      val words = lines.flatMap(_.split(" "))
      val pairs = words.map(word => (word, 1))
      val wordCounts = pairs.reduceByKey(_ + _)
      wordCounts.print()
      ssc.start()             // Start the computation
      ssc.awaitTermination()  // Wait for the computation to terminate
    }
}