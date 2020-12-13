package com.hypers.udf

import java.io.{BufferedReader, InputStream, InputStreamReader}

import org.apache.spark.sql.api.java.UDF1

import scala.collection.mutable

class UDFCleanCity extends UDF1[String, String] {

  private val cityMap = new mutable.HashMap[String, String]()
  private val cityList: Seq[String] = List()

  private val in1: InputStream = this.getClass.getClassLoader.getResourceAsStream("city.properties")
  private val in2: InputStream = this.getClass.getClassLoader.getResourceAsStream("city.txt")
  private val reader1 = new BufferedReader(new InputStreamReader(in1))
  private val reader2 = new BufferedReader(new InputStreamReader(in2))
  var line = "";
  while ((line = reader1.readLine()) != null) {
    val strs = line.split("=")
    cityMap.put(strs(0), strs(1))
  }
  while ((line = reader2.readLine()) != null) {
    cityList.+(line)
  }


  override def call(t1: String): String = {

    if (t1.trim.isEmpty)
      return "invalid"
    else if (cityMap.contains(t1))
      return cityMap.getOrElse(t1, "invalid")
    else {
      for (s <- cityList) {
        if (t1.contains(s))
          return s
      }
      return "invalid"
    }
  }
}

