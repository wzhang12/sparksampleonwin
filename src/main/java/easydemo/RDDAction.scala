package easydemo

/**
  * Created by zhangwen on 2016/3/8.
  */
import org.apache.spark._


object RDDAction {
  def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
      val sc = new SparkContext(conf)
      /*
      def take(num: Int): Array[T]

      take用于获取RDD中从0到num-1下标的元素，不排序。
       */
      val rddTake = sc.makeRDD(Seq(10, 4, 2, 12, 3))
      println("==================take=============")
      rddTake.take(1).foreach(println)
      println("==================take=============")
      /*
      def top(num: Int)(implicit ord: Ordering[T]): Array[T]
      top函数用于从RDD中，按照默认（降序）或者指定的排序规则，返回前num个元素。
      def takeOrdered(num: Int)(implicit ord: Ordering[T]): Array[T]
      takeOrdered和top类似，只不过以和top相反的顺序返回元素。
       */
      val rddTop = sc.makeRDD(Seq(10, 4, 2, 12, 3))
      implicit val myOrd = implicitly[Ordering[Int]].reverse
      println("==================top=============")
      rddTop.top(1).foreach(println)
      println("==================top=============")
      /*
      def aggregate[U](zeroValue: U)(seqOp: (U, T) ⇒ U, combOp: (U, U) ⇒ U)(implicit arg0: ClassTag[U]): U
      aggregate用户聚合RDD中的元素，先使用seqOp将RDD中每个分区中的T类型元素聚合成U类型，
      再使用combOp将之前每个分区聚合后的U类型聚合成U类型，特别注意seqOp和combOp都会使用zeroValue的值，zeroValue的类型为U。
      zeroValue即确定了U的类型，也会对结果产生至关重要的影响

      带入1第一分区应用第一个函数
      带入1第二分区应用第一个函数
      带入1上面两步应用第二个函数
      */
      val rddAggregate =sc.makeRDD(1 to 10,2)
      val rddAggregateDealed=rddAggregate.aggregate(1)(
                  {(a : Int,b : Int) => a + b},
                  {(a : Int,b : Int) => a + b}
      )
      println("==================aggregate=============")
      println(rddAggregateDealed)
      println("==================aggregate=============")

      /*
      def fold(zeroValue: T)(op: (T, T) ⇒ T): T
      fold是aggregate的简化，将aggregate中的seqOp和combOp使用同一个函数op。
      类型都是U
       */

      val rddLookUp = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("C",1)))
      println("==================lookup=============")
      rddLookUp.lookup("A").foreach(println)
      println("==================lookup=============")
      /*
      countByKey

      def countByKey(): Map[K, Long]
      countByKey用于统计RDD[K,V]中每个K的数量。

      def sortBy[K](f: (T) ⇒ K, ascending: Boolean = true, numPartitions: Int = this.partitions.length)(implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T]
      sortBy根据给定的排序k函数将RDD中的元素进行排序。
      默认true升序 false降序
       */
      val rddSortBy = sc.makeRDD(Array(("A",2),("A",1),("B",6),("B",3),("B",7)))
      println("==================sortBy=============")
      rddSortBy.sortBy(x => x._2,false).collect.foreach(println)
      println("==================sortBy=============")

      /*
      foreach
      accumulator
       */
      val rddForeach = sc.makeRDD(Array(("A",2),("A",1),("B",6),("B",3),("B",7)))
      println("==================Foreach=============")
      rddForeach.foreach({(x)=>println(x._1,x._2)})
      println("==================Foreach=============")

    sc.stop()
  }




}
