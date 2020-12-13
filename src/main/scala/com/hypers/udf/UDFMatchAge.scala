package com.hypers.udf

import org.apache.spark.sql.api.java.UDF1

class UDFMatchAge extends UDF1[Array[String], String] {
  override def call(t1: Array[String]): String = {
    if (t1.contains("D201")) {
      return "15-17"
    }
    else if (t1.contains("D202")) {
      return "18-19"
    }
    else if (t1.contains("D203")) {
      return "20-24"
    }
    else if (t1.contains("D204")) {
      return "25-29"
    }
    else if (t1.contains("D205")) {
      return "30-34"
    }
    else if (t1.contains("D206")) {
      return "35-39"
    }
    else if (t1.contains("D207")) {
      return "40-44"
    }
    else if (t1.contains("D208")) {
      return "45-49"
    }
    else if (t1.contains("D209")) {
      return "50-54"
    }
    else if (t1.contains("D210")) {
      return "55+"
    }
    else if (t1.contains("T030601")) {
      return "15-17"
    }
    else if (t1.contains("T030602")) {
      return "20-24"
    }
    else if (t1.contains("T030603")) {
      return "25-29"
    }
    else if (t1.contains("T030604")) {
      return "30-34"
    }
    else if (t1.contains("T030605")) {
      return "35-39"
    }
    else if (t1.contains("T030606")) {
      return "40-44"
    }
    else if (t1.contains("T030607")) {
      return "45-49"
    }
    else if (t1.contains("T030608")) {
      return "50-54"
    }
    else if (t1.contains("A190100")) {
      return "20-24"
    }
    else if (t1.contains("A190101")) {
      return "25-29"
    }
    else if (t1.contains("A190102")) {
      return "35-39"
    }
    else if (t1.contains("A190103")) {
      return "45-49"
    }
    else return "invalid"
  }
}
