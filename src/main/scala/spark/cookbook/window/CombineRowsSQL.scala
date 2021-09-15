package window

import spark.Setup.sparkLocal

object CombineRowsSQL {

  case class LogGrouping(id: Long, file: String, logs: Seq[Log])
  case class Info(a: String, b: String, c: String, file: String, id: Long)

  def main(args: Array[String]): Unit = {

    import sparkLocal.implicits._

    ExampleData.Get().createOrReplaceTempView("logs")

    val infoDf = sparkLocal.sql(
      """ with indicators as (
        |   select
        |     struct(message, file, offset) as log,
        |     sum(if (message like '%Started%', 1, 0)) over(PARTITION BY file ORDER BY offset) as groupingId
        |   from logs)
        | select
        |   groupingId as id,
        |   log.file as file,
        |   collect_list(log) as logs
        | from indicators
        | group by groupingId, log.file
        |""".stripMargin).as[LogGrouping].map(x => {

      val a = x.logs.find(_.message.contains("Info A")).head.message.split(" ").last
      val b = x.logs.find(_.message.contains("Info B")).head.message.split(" ").last
      val c = x.logs.find(_.message.contains("Info C")).head.message.split(" ").last

      Info(a, b, c, x.file, x.id)
    })

    infoDf.show(truncate = false)
  }
}
