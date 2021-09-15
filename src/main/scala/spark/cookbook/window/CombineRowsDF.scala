package window

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import spark.Setup.sparkLocal

object CombineRowsDF {

  case class LogGroupingIndicator(log: Log, isIndicator: Long, groupingId: Long)
  case class LogGrouping(id: Long, file: String, logs: Seq[Log])
  case class Info(a: String, b: String, c: String, file: String, id: Long)

  def main(args: Array[String]): Unit = {

    import sparkLocal.implicits._

    val window = Window.partitionBy($"log.file").orderBy($"log.offset")

    val infoDf = ExampleData.Get()
      .map(x => {
        val isIndicator = if (x.message.contains("Started")) 1 else 0
        LogGroupingIndicator(x, isIndicator, 0)
      }).withColumn("groupingId", sum($"isIndicator").over(window))
      .groupBy($"groupingId".as("id"),
        $"log.file".as("file")).agg(collect_list($"log").as("logs"))
      .as[LogGrouping].map(x => {
      val a = x.logs.find(_.message.contains("Info A")).head.message.split(" ").last
      val b = x.logs.find(_.message.contains("Info B")).head.message.split(" ").last
      val c = x.logs.find(_.message.contains("Info C")).head.message.split(" ").last

      Info(a, b, c, x.file, x.id)
    })

    infoDf.show(truncate = false)
  }
}
