package top.jsoul.test

import java.util.HashMap

import org.apache.spark.sql.SparkSession

object Test {

  def main(args: Array[String]): Unit = {

//    val spark = SparkSession.builder()
//        .master("local[8]")
//        .getOrCreate()
//    val acc = new MyAcc
//    spark.sparkContext.register(acc)
//
//
//    val value = spark.sparkContext.parallelize(Array("1", "2", "3", "4")).map(x => {
//      acc.add(x)
//      x + "test"
//    })
//    value.collect().foreach(println)
//    println("----------------")
//
//    val md5ToKey = new HashMap[String, String]()
//    acc.value.foreach(str => {
//      md5ToKey.put(str+"test", str)
//    })
//
//    val strings = md5ToKey.keySet()
//    strings.toArray.foreach(str=>{
//      println(str+":"+md5ToKey.get(str))
//    })
//
//    spark.close()

    println("abcd".substring(2,3))
  }
}
