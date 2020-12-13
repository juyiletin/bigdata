package top.jsoul.movie

import org.apache.spark.sql.SparkSession


object DataLoad {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .enableHiveSupport()
      .getOrCreate()

    val name_basics = spark.read.option("sep", "\t").option("header", true).csv("file:///E:/IdeaProjects/bigdata/src/main/resources/movie_data/name.basics.tsv.gz")

    name_basics.coalesce(1).write.saveAsTable("movie.name_basics")

    val title_akas = spark.read.option("sep", "\t").option("header", true).csv("file:///E:/IdeaProjects/bigdata/src/main/resources/movie_data/title.akas.tsv.gz")

    title_akas.coalesce(1).write.saveAsTable("movie.title_akas")

    val title_basics = spark.read.option("sep", "\t").option("header", true).csv("file:///E:/IdeaProjects/bigdata/src/main/resources/movie_data/title.basics.tsv.gz")

    title_basics.coalesce(1).write.saveAsTable("movie.title_basics")

    val title_crew = spark.read.option("sep", "\t").option("header", true).csv("file:///E:/IdeaProjects/bigdata/src/main/resources/movie_data/title.crew.tsv.gz")

    title_crew.coalesce(1).write.saveAsTable("movie.title_crew")

    val title_episode = spark.read.option("sep", "\t").option("header", true).csv("file:///E:/IdeaProjects/bigdata/src/main/resources/movie_data/title.episode.tsv.gz")

    title_episode.coalesce(1).write.saveAsTable("movie.title_episode")

    val title_principals = spark.read.option("sep", "\t").option("header", true).csv("file:///E:/IdeaProjects/bigdata/src/main/resources/movie_data/title.principals.tsv.gz")

    title_principals.coalesce(1).write.saveAsTable("movie.title_principals")

    val title_ratings = spark.read.option("sep", "\t").option("header", true).csv("file:///E:/IdeaProjects/bigdata/src/main/resources/movie_data/title.ratings.tsv.gz")

    title_ratings.coalesce(1).write.saveAsTable("movie.title_ratings")

    spark.close()
  }

}
