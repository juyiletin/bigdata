package top.jsoul

import org.apache.spark.sql.{Row, SparkSession}

object HelloWorld {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    val frame = spark.read.parquet("/user/hive/warehouse/movie.db/title_principals")
    val row: Row = frame.first()
    println(row.getString(0))

    spark.close()

  }
}
