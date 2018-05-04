import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row;

object CommentsParquetApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("CommentsParquet").getOrCreate() 

    // Read the posts
    val xmlPosts = spark.sparkContext.newAPIHadoopFile("/user/edureka_162051/stackoverflow_ds/Comments.xml", classOf[TextInputFormat], classOf[LongWritable], classOf[Text])

    // Get only the node as org.apache.hadoop.io.Text, convert to string, get only row nodes and convert to scala.xml.Elem
    val eachComment = xmlPosts.map({case (x, y) => (y.toString.trim)}).filter(_.contains("<row ")).map(x => scala.xml.XML.loadString(x.toString))

    // Create a tuple with only the necessary fields
    val commentFields = eachComment.map(createRecord)

    // Create a Schema
    val commentsSchema = 
    StructType(
      Array(
        StructField("Id",IntegerType),
        StructField("PostId",IntegerType),
        StructField("Score",IntegerType),
        StructField("CreationDate",StringType),
        StructField("UserId",IntegerType)))

    // Create a DataFrame
    val commentsDF = spark.createDataFrame(commentFields, commentsSchema)

    // Write as CSV to HDFS
    commentsDF.write.parquet("/user/edureka_162051/stackoverflow_ds/comments_parquet")
    }

  def createRecord(nd : scala.xml.Elem) = {
    val c =  (nd \ "@Id").toString   
    val p = (nd \ "@PostId").toString
    val s = (nd \ "@Score").toString
    val cd = (nd \ "@CreationDate").toString  
    val u =  (nd \ "@UserId").toString   

    val commentId =  if (c.isEmpty) null else c.toInt  
    val postId =  if (p.isEmpty) null else p.toInt  
    val score =  if (s.isEmpty) null else s.toInt  
    val creationDate =  if (cd.isEmpty) null else cd  
    val userId =  if (u.isEmpty) null else u.toInt  

    Row(commentId, postId, score, creationDate, userId)
  }
}
