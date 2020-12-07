import fr.esme.gdpr.DataFrameReader
import fr.esme.gdpr.configuration.{ConfigReader, JsonConfig}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import spray._
import spray.json._
import fr.esme.gdpr.configuration.JsonConfig._
import fr.esme.gdpr.configuration.JsonConfigProtocol._
import fr.esme.gdpr.utils.schemas.DataFrameSchema
object Launcher {
  def main(args: Array[String]): Unit = {
    //Add Scopt command line
    Logger.getLogger("org").setLevel(Level.OFF)

    implicit val session = SparkSession.builder().master("local").getOrCreate()

    val confReader = ConfigReader.readConfig("conf/config.json")
    val configuration = confReader.parseJson.convertTo[JsonConfig]
    val dfSchema: StructType = DataFrameSchema.buildDataframeSchema(configuration.fields)
    val data = DataFrameReader.readCsv("data.csv", configuration.csvOptions, dfSchema)
    data.printSchema()
    data.show

  }
}
