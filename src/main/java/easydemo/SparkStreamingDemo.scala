package easydemo

/**
  * Created by zhangwen on 2016/3/9.
  */
object SparkStreamingDemo {
    def main(args:Array[String]){
      import org.apache.spark._
      import org.apache.spark.streaming._

      // Create a local StreamingContext with two working thread and batch interval of 1 second.
      // The master requires 2 cores to prevent from a starvation scenario.

      val conf = new SparkConf().setMaster("local[4]").setAppName("NetworkWordCount")
      val ssc = new StreamingContext(conf, Seconds(1))
      val lines = ssc.socketTextStream("115.28.204.97", 9998)
      val words = lines.flatMap(_.split(" "))
      val pairs = words.map(word => (word, 1))
      /*
      reduceByKey(binary_function)
      reduceByKey就是对元素为KV对的RDD中Key相同的元素的Value进行binary_function的reduce操作，因此，Key相同的多个元素的值被reduce为一个值，然后与原RDD中的Key组成一个新的KV对。
       */
//      val wordCounts = pairs.reduceByKey(_ + _)
//      wordCounts.print()

      /*
      如何统计总共的每个record的每个单词的出现的次数 用updateStateByKey
      这个方法对比1.6版本出的mapWithState性能已经差了许多
       */
      ssc.checkpoint("D:\\workspace\\tesr2\\sparksampleonwin\\checkpointFile")
      val updateFunc = (values: Seq[Int], state: Option[Int]) => {


        println("=================================",state)
        val currentCount = values.sum
        println("=================================c",currentCount)
        val previousCount = state.getOrElse(0)
        Some(currentCount + previousCount)
      }

      pairs.updateStateByKey(updateFunc).print()

      /*
      transform 一个DStream包含多个rdd，这个方法会将多个rdd应用于传入的方法
      RDD中的许多方法DStream中是没有的
      PairDStreamFunctions中是含有join
      但一般的DStream中不含有join
       */

      /*
      窗口操作
      每隔10秒，统计一下过去30秒过来的数据
      val windowedWordCounts = pairs.reduceByKeyAndWindow(_ + _, Seconds(30), Seconds(10))
       */
//      val windowedWordCounts = pairs.reduceByKeyAndWindow((a:Int,b:Int) => (a + b),Seconds(30), Seconds(10))
//      windowedWordCounts.print();

      ssc.start()             // Start the computation
      ssc.awaitTermination()  // Wait for the computation to terminate
      ssc.stop()
    }

}
