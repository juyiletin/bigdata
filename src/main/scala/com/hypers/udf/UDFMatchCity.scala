package com.hypers.udf

import org.apache.spark.sql.api.java.UDF1

class UDFMatchCity extends UDF1[Array[String], String] {
  override def call(t1: Array[String]): String = {
    if (t1.contains("A190139")) {
      return "1"
    }
    else if (t1.contains("A190140")) {
      return "2"
    }
    else if (t1.contains("A190141")) {
      return "3"
    }
    else if (t1.contains("A190142")) {
      return "4"
    }
    else return "invalid"
  }
}
