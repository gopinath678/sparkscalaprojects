import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row;

object PostLinksHeaderCSVApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("PostLinksHeaderCSVApp").getOrCreate() 

    // Read the posts
    val xmlPosts = spark.sparkContext.newAPIHadoopFile("/user/edureka_162051/stackoverflow_ds/PostLinks.xml", classOf[TextInputFormat], classOf[LongWritable], classOf[Text])

    // Get only the node as org.apache.hadoop.io.Text, convert to string, get only row nodes and convert to scala.xml.Elem
    val eachPost = xmlPosts.map({case (x, y) => (y.toString.trim)}).filter(_.contains("<row ")).map(x => scala.xml.XML.loadString(x.toString))

    // Create a tuple with only the necessary fields
    val postFields = eachPost.map(createRecord)

    //Create schema
    val postLinksSchema = 
    StructType(
      Array(
        StructField("Id",StringType),
        StructField("CreationDate",StringType),
        StructField("PostId",StringType),
        StructField("RelatedPostId",StringType),
        StructField("LinkTypeId",StringType)))

    // Create a DataFrame
    val postDF = spark.createDataFrame(postFields, postLinksSchema)

    // Write as CSV to HDFS
    postDF.write.option("header","true").format("com.databricks.spark.csv").save("/user/edureka_162051/stackoverflow_ds/postlink_header_csv")
    }

// extract the values in each xml row
def createRecord(nd : scala.xml.Elem) = {
  val postLinkId = (nd \ "@Id").toString
  val creationdate = (nd \ "@CreationDate").toString
  val postId = (nd \ "@PostId").toString
  val relatedPostId = (nd \ "@RelatedPostId").toString
  val linkTypeId = (nd \ "@LinkTypeId").toString
  Row(postLinkId,creationdate,postId,relatedPostId,linkTypeId)
  }
}
