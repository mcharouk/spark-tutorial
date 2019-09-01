package marc.tutorial

import marc.tutorial.entities.{Person, RoleCount}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkBasicExample {


  def main(args: Array[String]): Unit = {
    //create Spark Session
    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("SparkBasicExample")
      .config("spark.sql.shuffle.partitions", 1)
      .enableHiveSupport()
      .getOrCreate()

    import sparkSession.implicits._

    //read Source
    val teamDF = sparkSession.read
      .option("delimiter", ";")
      .option("header", true)
      .csv("src/main/resources/team.csv")
      .as[Person]

    //Do The Job
    val resultDF = teamDF
      .where(not('role === lit("scrum")))
      .groupBy("role")
      .count()

//        teamDF.registerTempTable("TEAM")
//        val query = "SELECT role, count(*) as count " +
//          "FROM TEAM " +
//          "WHERE role != 'scrum' " +
//          "GROUP BY role"
//        val resultDF = sparkSession.sql(query)

//        val resultDF = teamDF
//          .filter(person => person.role != "scrum")
//          .groupByKey(person => person.role)
//          .mapGroups((role, personIterator) => RoleCount(role, personIterator.size))

    //Write in Sink
    resultDF
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .option("delimiter", "|")
      .option("header", true)
      .save("src/main/resources/aggTeam")

    sparkSession.close()
  }


  //  private def partition(df: DataFrame): DataFrame ={
  //    df
  //      //when we want to lower partition number
  //      .coalesce(4)
  //      //when we want to add more partitions
  //      .repartition(6)
  //
  //      //it's the same
  //      .cache()
  //      .persist(StorageLevel.MEMORY_AND_DISK)
  //  }


//    private def broadcast(sparkSession: SparkSession, df1: DataFrame, df2: DataFrame): DataFrame ={
//      import sparkSession.implicits._
//      df1.join(broadcast(df2), 'df1_code === 'df2_code, "left_outer")
//    }

}
