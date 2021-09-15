package window

import org.apache.spark.sql.Dataset
import spark.Setup.sparkLocal

case class Log(message: String, file: String, offset: Long)

object ExampleData {

  def Get(): Dataset[Log] = {

    import sparkLocal.implicits._

    var offset = 0;
    val logDf = """ 00:05:01 Started
                  | 00:05:02 Info A A1
                  | 00:05:03 Info B B1
                  | 00:05:04 Info C C1
                  | 00:06:01 Started
                  | 00:05:02 Info A A2
                  | 00:05:03 Info B B2
                  | 00:05:04 Info C C2
                  |""".stripMargin.split("\r\n").map(x => {
      offset += 1
      Log(x, "example.log", offset)
    }).toSeq.toDS()

    logDf
  }
}
