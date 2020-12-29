package top.jsoul.movie

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object Test {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("Test")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val rdd1: RDD[(Double, (Long, String))] = spark.read.table("movie.title_ratings")
      .map(row => {
        (row.getString(1).toDouble, (row.getString(2).toLong, row.getString(0)))
      })
      .rdd

    val rdd2 = rdd1
      .groupByKey(20)
      .flatMap(tuple => {
        val tuples = tuple._2.toSeq.sortBy(tp => tp._1).reverse
        var i = 0
        for (elem <- tuples)
          yield {
            i += 1
            (tuple._1, elem, i)
          }
      })

    rdd2
        .sortBy(tuple=>{
          tuple._3
        })
      .sortBy(tuple=>{
        tuple._1
      },false)
      .filter(tuple=>{
      tuple._3.<=(3)
    }).take(1000).foreach(row => println(row.toString()))

    spark.close()
  }

  case class rate(tconst: String, rating: Double, votes: Long)

}
