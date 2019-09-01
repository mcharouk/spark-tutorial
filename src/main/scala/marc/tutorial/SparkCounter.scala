package marc.tutorial

import org.apache.spark.sql.SparkSession

object SparkCounter {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("SparkCounter")
      .enableHiveSupport()
      .getOrCreate()

    var counter = 0

    //generate a df with 3 items
    sparkSession.range(1, 4)
      //for each line (3 lines), increment counter
      .foreach(_ => counter = counter + 1)

    println(s"counter : $counter")

    sparkSession.close()
  }

}
