package spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Setup {

  Logger.getRootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

  val sparkLocal: SparkSession = SparkSession.builder().master("local[2]").getOrCreate()
}
