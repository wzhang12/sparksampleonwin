package easydemo

/**
  * Created by zhangwen on 2016/3/8.
  */
import org.apache.spark._
import org.apache.spark.streaming.dstream.DStream

//统计字符出现次数
object RDDTransformation {
  def main(args: Array[String]) {
      val logFile = "D:\\测试文本\\wordcontain.txt" // Should be some file on your system
      val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
      val sc = new SparkContext(conf)
      /*
          1 textFile 方法也可以通过输入一个可选的第二参数，来控制文件的分片数目。
          默认情况下，Spark 为每一块文件创建一个分片（HDFS 默认的块大小为64MB)，
          但是你也可以通过传入一个更大的值，来指定一个更高的片值，但不能指定一个比块数更小的片值。

          2 Spark可以将数据集存放在集群中的缓存中
      */
      val logData = sc.textFile(logFile, 2).cache()
      val numAs = logData.filter(line => line.contains("a")).count()
      val numBs = logData.filter(line => line.contains("b")).count()
      println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))


      val line = sc.textFile("D:\\测试文本\\wordcontain.txt")
      line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).collect().foreach(println)
      /*
      aggregate
      1 parallelize将结果转成RDD数据结构并分区
      2 aggregate会首先将zero-value即3与一个分区中的一个值应用于函数seqOP返回类型与zero-value一致，再
        将结果重新与当前分区的另一个数字应用于seqOP。下个分区也执行相同流程，直到所有分区执行完。每个分区会产生一个结果
        所有结果可以看成在一个分区，也执行上面流程只不过执行的是combOp方法
       */
      val z = sc. parallelize ( List (1 ,2 ,3 ,4 ,5 ,6) , 2)

      println(z. aggregate(3)(seqOP, combOp))
     /*
     fold
     fold类似于aggregate,只不过seqOp和combOp是同一个函数
      */
     println(z.fold(7)(combOp))
     println(z.fold(7)(combOp))
    /*
    lookup
    lookup用于(K,V)类型的RDD,指定K值，返回RDD中该K对应的所有V值
     */

    var rdd1 = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("C",1)))
    println(rdd1.lookup("A"))

    /*
    TODO:preferredLocations
     */

    /*
    coalesce(repartition)
    coalesce(4,true)=repartition(4)
    该函数用于将RDD进行重分区，使用HashPartitioner。
    第一个参数为重分区的数目，第二个为是否进行shuffle，默认为false，rdd默认两个分区
    如果重分区的数目大于原来的分区数，那么必须指定shuffle参数为true，否则，分区数不变 TODO：为什么
     */
    println(z.partitions.size)
    println(z.repartition(4))
    //repartition returns a new RDD with the partitioning changed
    println(z.repartition(4).partitions.size)
    /*
    union:简单的合并
    intersection:该函数返回两个RDD的交集，并且去重。参数numPartitions指定返回的RDD的分区数。参数partitioner用于指定分区函数
    subtract：返回在调用RDD中出现，并且不在函数传入的RDD中出现的元素，不去重。
     */

    /*
    mapPartitions是粗粒度的map，x是一个分区中的集合
    返回的是一个iterator可迭代的对象
     */
    val rddMapPartition = sc.makeRDD(1 to 5,2)
    val rddMapPartitionDealed = rddMapPartition.mapPartitions{x=>{
    val result = List[Int]()
         var i = 0
         while(x.hasNext){
            i += x.next()
          }
         result.::(i).iterator
     }}
    println("=======mapPartition==============")
    rddMapPartitionDealed.collect.foreach(println)
    println("=======mapPartition==============")
    /*
    mapPartitionsWithIndex在mapPartitions上加了分区的索引x
     */
    val rddMapPartitionIndex = sc.makeRDD(Array(("A",1),("A",2),("B",1),("B",2),("B",3),("C",1),("C",2)))
    val rddMapPartitionIndexDealed = rddMapPartitionIndex.mapPartitionsWithIndex{

      (x,iter) => {
        var result = List[String]()
        while(iter.hasNext){
          val item=iter.next
          result=result.::(x + "|" + item._1+"|"+item._2)
        }
        result.iterator
      }

    }
    println("=======mapPartitionsWithIndex==============")
    println("partitions size "+rddMapPartitionIndex.partitions.size)
    rddMapPartitionIndexDealed.collect.foreach(println)
    println("=======mapPartitionsWithIndex==============")




    /*
    zip将两个rdd整合成(key,value)的形式，如果两个rdd中的分区不相等会报错
     */
    var rddZip0 = sc.makeRDD(1 to 5,2)
    var rdd1ip1 = sc.makeRDD(Seq("A","B","C","D","E"),2)
    println(rddZip0.zip(rdd1ip1).collect())

    /*
    zipPartition整合两个或三个
    Similar to zip. But provides more control over the zipping process.
     */
    var rddZipPart0 = sc.makeRDD(1 to 5,2)
    var rddZipPart1 = sc.makeRDD(Seq("A","B","C","D","E"),2)
    val resultZip=rddZipPart0.zipPartitions(rddZipPart1){
         (rdd1Iter,rdd2Iter) => {
             var result = List[String]()
             while(rdd1Iter.hasNext && rdd2Iter.hasNext) {
                 result::=(rdd1Iter.next() + "_" + rdd2Iter.next())
               }
           result.iterator
         }
         }.collect()
    println("=======zipPartitions==============")
    resultZip.foreach(println)
    println("=======zipPartitions==============")

    /*
    mapValues()操作，mapValues是针对[K,V]中的V值进行map操作，能穿个函数修改
     */
    val rddMapValues = sc.makeRDD(Array((1,"A"),(2,"B"),(3,"C"),(4,"D")),2)
    println("=======mapValues==============")
    rddMapValues.mapValues(x=>x+'_').collect().foreach(println)
    println("=======mapValues==============")

    /*
    flatMapValues()会破坏参数里的结构，然后与key去匹配,x是value
     */
    val rddflatMapValues = sc.makeRDD(Array((1,"A"),(2,"B"),(3,"C"),(4,"D")),2)
    println("=======flatMapValues==============")
    rddflatMapValues.flatMapValues(x=>x+'_'+'$').collect().foreach(println)
    println("=======flatMapValues==============")

    /*
    combineByKey
    0|A|1
    1|B|1
    1|A|2
    2|B|3
    2|B|2
    3|C|2
    3|C|1
    默认分区是这样的，分区不同最后的结果也会不一样
    当在一个分区第一次遇到key的时候会执行方法一，如果在这个分区仍然出现会执行方法二，当每个分区都执行完了
    会以相同key的执行方法三
     */
    val rddCombineByKey = sc.makeRDD(Array(("A",1),("A",2),("B",1),("B",2),("B",3),("C",1),("C",2)))
    println("=======CombineByKey==============")
    println("======="+rddCombineByKey.partitions.size+"==============")
    rddCombineByKey.combineByKey(
           (v : Int) => v + "_",
           (c : String, v : Int) => c + "#" + v,
           (c1 : String, c2 : String) => c1 + "$" + c2
         ).collect.foreach(println)
    println("=======CombineByKey==============")

    /*
    foldByKey
    该函数用于RDD[K,V]根据K将V做折叠、合并处理，其中的参数zeroValue表示先根据映射函数将zeroValue应用于V,进行初始化V,再将映射函数应用于初始化后的V.
     */
    var rddFoldByKey = sc.makeRDD(Array(("A",0),("A",2),("B",1),("B",2),("C",1)))
    println("=======foldByKey==============")
    rddFoldByKey.foldByKey(0)(_+_).collect.foreach(println)
    println("=======foldByKey==============")

    /*
    groupByKey 将相同key的value整合进CompactBuffer 返回RDD[(K, Iterable[V])]
    reduceByKey
    reduceByKeyLocally不能指定分区不能指定分区器 返回的是map
    cogroup相当于SQL中的全外关联full outer join 但是与reduceBykey每个value都在一个CompactBuffer中，这里每个value都在一个CompactBuffer中
    join相当于SQL中的内关联join，只返回两个RDD根据K可以关联上的结果，join只能用于两个RDD之间的关联，如果要多个RDD关联，多关联几次即可。

    cogroup和join对应返回的RDD[(K, (Iterable[V], Iterable[W]))]和RDD[(K, (V, W))]
    A leftOuterJoin B 保留a有b没有的 但不保留b有a没有的
    A rightOuterJoin B
    A subtractByKey B

     */

    sc.stop()
  }

  def seqOP(a:Int, b:Int) : Int = {
     println("seqOp: " + a + "\t" + b)
     math.min(a,b)
     }
  def combOp(a:Int, b:Int): Int = {
     println("combOp: " + a + "\t" + b)
     a + b
     }


}
