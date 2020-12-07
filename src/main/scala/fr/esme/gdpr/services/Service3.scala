package fr.esme.gdpr.services

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Service3 {
  def getClientData(df: DataFrame, id: String, idColumnName: String, writePath: String): Unit = {
    df.filter(col(idColumnName) === id).write.csv(writePath + "_" + id)
  }
}
