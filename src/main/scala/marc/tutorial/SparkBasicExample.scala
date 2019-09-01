package marc.tutorial

import marc.tutorial.entities.Person
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object SparkBasicExample {


  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("SparkBasicExample")
      .config("spark.sql.shuffle.partitions", 1)
      .enableHiveSupport()
      .getOrCreate()

    import sparkSession.implicits._

    val teamDF = sparkSession.read
      .option("delimiter", ";")
      .option("header", true)
      .csv("src/main/resources/team.csv")
      .as[Person]

    val resultDF = teamDF
      .where(not('role === lit("scrum")))
      .groupBy("role")
      .count()

    //    teamDF.registerTempTable("TEAM")
    //    val resultDF = sparkSession.sql("SELECT role, count(*) as count FROM TEAM WHERE role != 'scrum' GROUP bY role")

    //    val resultDF = teamDF
    //      .filter(person => person.role != "scrum")
    //      .groupByKey(person => person.role)
    //      .mapGroups((role, personIterator) => RoleCount(role, personIterator.size))

    resultDF
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .option("delimiter", "|")
      .option("header", true)
      .save("src/main/resources/aggTeam")

    sparkSession.close()
  }

}
