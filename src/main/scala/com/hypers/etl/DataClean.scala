package com.hypers.etl

import com.hypers.udf.{UDFCleanCity, UDFMatchAge, UDFMatchCity, UDFMatchGender}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

object DataClean {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("yarn")
      .appName("DataClean")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    spark.udf.register("city_clean", new UDFCleanCity, DataTypes.StringType)
    spark.udf.register("city_match", new UDFMatchCity, DataTypes.StringType)
    spark.udf.register("age_match", new UDFMatchAge, DataTypes.StringType)
    spark.udf.register("gender_match", new UDFMatchGender, DataTypes.StringType)
    val match_city = udf(new UDFMatchCity, DataTypes.StringType)
    val match_age = udf(new UDFMatchAge, DataTypes.StringType)
    val match_gender = udf(new UDFMatchGender, DataTypes.StringType)

    val source = spark.read.table("app.sr_tmp_test_for_clean_v2")
    val tag = spark.read.table("app.sr_clean_pn_third_tag")
    val city = spark.read.table("app.sr_clean_city_tier")

    source.persist(StorageLevel.MEMORY_AND_DISK)

    val invalid = source.filter(source("pn_clean").===("invalid"))
    val valid = source.filter(source("pn_clean").=!=("invalid"))
    val tag_tmp = tag.map(row => {
      (row.getString(0), row.getString(1).split(","))
    }).toDF("pn", "tags")

    val valid_tag: DataFrame = valid.join(tag_tmp, valid("pn_clean").===(tag_tmp("pn")), "left")
      .select(valid("brand"),
        valid("person_id"),
        valid("birth_dt"),
        valid("age"),
        valid("gender"),
        valid("gender_clean"),
        valid("city"),
        valid("city_clean"),
        valid("pn"),
        valid("pn_clean"),
        tag_tmp("tags"))

    val valid_tmp: DataFrame = valid_tag.na.fill("invalid")
      .map(row => {
        var age = row.getString(3)
        var gender_clean = row.getString(5)
        var city_clean = row.getString(7)
        if (age == "invalid") {
          age = match_age(valid_tag("tags")).toString()
        }
        if (gender_clean == "invalid") {
          gender_clean = match_gender(valid_tag("tags")).toString()
        }
        if (city_clean == "invalid") {
          city_clean = match_city(valid_tag("tags")).toString()
        }
        Row(row.getString(0),
          row.getString(1),
          row.getString(2),
          age,
          row.getString(4),
          gender_clean,
          row.getString(6),
          city_clean,
          row.getString(8),
          row.getString(9))
      })

    val tag_matched = valid_tmp.union(invalid).toDF("brand", "person_id", "birth_dt", "age", "gender", "gender_clean", "city", "city_clean", "pn", "pn_clean")

    val city_tmp = tag_matched.filter(!tag_matched("city_clean").isin(List("invalid", "1", "2", "3", "4")))
    val city_other = tag_matched.filter(tag_matched("city_clean").isin(List("invalid", "1", "2", "3", "4")))

    val city_matched = city_tmp.join(city, city_tmp("city_clean").===(city("city")), "left")
      .select(city_tmp("brand"),
        city_tmp("person_id"),
        city_tmp("birth_dt"),
        city_tmp("age"),
        city_tmp("gender"),
        city_tmp("gender_clean"),
        city_tmp("city"),
        city("tier"),
        city_tmp("pn"),
        city_tmp("pn_clean"))

    val res = city_matched.union(city_other)
      .toDF("brand", "person_id", "birth_dt", "age", "gender", "gender_clean", "city", "tier", "pn", "pn_clean")

    res.write.saveAsTable("app.sr_tmp_test_for_clean_v5")

    spark.close()
  }

}
