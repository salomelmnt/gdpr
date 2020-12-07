import fr.esme.gdpr.services.{Service1, Service2, Service3}
import org.apache.spark.sql.SparkSession

object Launcher {
  def main(args: Array[String]): Unit = {
    //Add Scopt command line
    val serviceToLaunch = "s1"

    val session = SparkSession.builder().master("local").getOrCreate()
    val df = session.read.csv("inputPath")
    serviceToLaunch match {
      case "s1" => Service1.deleteLineWithId(df, "", "")
      case "s2" => Service2.hashIdColumn(df, "", "", "")
      case "s3" => Service3.getClientData(df, "", "", "")
    }
  }
}
