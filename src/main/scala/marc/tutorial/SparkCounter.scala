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

   sparkSession.range(1,4)
       .foreach(number => {
         println(s"current Number : $number")
         counter = counter + 1
       })

    println(s"counter : $counter")

    sparkSession.close()
  }

}
