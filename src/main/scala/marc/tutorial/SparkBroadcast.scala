package marc.tutorial

import org.apache.spark.sql.SparkSession

object SparkBroadcast {


  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("SparkBroadcast")
      .enableHiveSupport()
      .getOrCreate()

    val counter = sparkSession.sparkContext.longAccumulator("counter")
    val increment = sparkSession.sparkContext.broadcast(5)

    sparkSession.range(1,4)
      .foreachPartition(numberIterator => {
        for (number <- numberIterator) counter.add(increment.value)
      })

    println(s"counter : ${counter.value}")

    sparkSession.close()
  }

}
