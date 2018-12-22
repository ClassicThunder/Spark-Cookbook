package spark.cookbook.windows

import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

class WindowsTest extends FlatSpec {

   val spark = SparkSession.builder
     .master("local")
     .appName("Simple Application")
     .getOrCreate()

  "A Stack" should "pop values in last-in-first-out order" in {
    assert(0 == 0)
  }
}
