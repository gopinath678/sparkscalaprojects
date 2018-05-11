import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
object SparkKafkaApp extends App{
  val topics = Map("topics"->1)
  val conf = new SparkConf().setMaster("local[2]").setAppName("simpleApp")
  val ssc = new StreamingContext(conf, Seconds(10))
  val lines = KafkaUtils.createStream(ssc , "ip-20-0-21-161:2181","default",topics).map(_._2)
  val result =  lines.flatMap(x=>x.split(" ")).map((_,1)).reduceByKey(_ + _)
  result.print
  ssc.start()
  ssc.awaitTermination()
}
