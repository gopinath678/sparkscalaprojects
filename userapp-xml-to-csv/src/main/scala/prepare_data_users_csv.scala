import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input._
import scala.util.matching.Regex

object PrepareUsersApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("PrepareUsersCSV").getOrCreate() 

    // Read the posts
    val xmlUsers = spark.sparkContext.newAPIHadoopFile("/user/edureka_162051/stackoverflow_ds/Users.xml", classOf[TextInputFormat], classOf[LongWritable], classOf[Text])

    // Get only the node as org.apache.hadoop.io.Text, convert to string, get only row nodes and convert to scala.xml.Elem
    val eachUser = xmlUsers.map({case (x, y) => (y.toString.trim)}).filter(_.contains("<row ")).map(x => scala.xml.XML.loadString(x.toString))

    // Create a tuple with only the necessary fields
    val postFields = eachUser.map(createRecord)

    // Create a DataFrame
    val userDF = spark.createDataFrame(postFields)

    // Write as CSV to HDFS
    userDF.write.format("com.databricks.spark.csv").save("/user/edureka_162051/stackoverflow_ds/users_csv")
    }

// extract the values in each xml row
def createRecord(nd : scala.xml.Elem) = {
  val Id = (nd \ "@Id").toString
  val Reputation = (nd \ "@Reputation").toString
  val CreationDate = (nd \ "@CreationDate").toString
  val DisplayName = (nd \ "@DisplayName").toString
  val LastAccessDate = (nd \ "@LastAccessDate").toString 
  val Views = (nd \ "@Views").toString
  val UpVotes = (nd \ "@UpVotes").toString
  val DownVotes = (nd \ "@DownVotes").toString
  val Age = (nd \ "@Age").toString
  val AccountId = (nd \ "@AccountId").toString
  (Id, Reputation, CreationDate, DisplayName, LastAccessDate, Views, UpVotes, DownVotes, Age, AccountId)
  }
}
