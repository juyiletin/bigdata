package com.hypers.udf

import org.apache.spark.sql.api.java.UDF1

class UDFMatchGender extends UDF1[Array[String], String] {
  override def call(t1: Array[String]): String = {
    if (t1.contains("D101")) {
      return "M"
    }
    else if (t1.contains("D102")) {
      return "F"
    }
    else if (t1.contains("T030101")) {
      return "M"
    }
    else if (t1.contains("T030102")) {
      return "F"
    }
    else if (t1.contains("A190105")) {
      return "M"
    }
    else if (t1.contains("A190104")) {
      return "F"
    }
    else return "invalid"
  }
}
