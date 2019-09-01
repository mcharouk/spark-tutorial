package marc.tutorial

import org.apache.spark.sql.SparkSession

object SparkAccumulator {


  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("SparkAccumulator")
      .enableHiveSupport()
      .getOrCreate()

    val counter = sparkSession.sparkContext.longAccumulator("counter")

    sparkSession.range(1,4)
      .foreachPartition(numberIterator => {
        for (number <- numberIterator) counter.add(1)
      })

    println(s"counter : ${counter.value}")

    sparkSession.close()
  }

}
