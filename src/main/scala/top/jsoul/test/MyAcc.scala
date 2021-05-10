package top.jsoul.test

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class MyAcc extends AccumulatorV2[String, mutable.HashSet[String]] {

  private var myacc = new mutable.HashSet[String]()
  myacc.add("test")

  override def isZero: Boolean = {
    myacc.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashSet[String]] = {
    val acc = new MyAcc
    acc.myacc = myacc
    acc
  }

  override def reset(): Unit = {
    myacc.clear()
  }

  override def add(v: String): Unit = {
    if (null != v){
      myacc.add(v)
    }
  }

  override def merge(other: AccumulatorV2[String, mutable.HashSet[String]]): Unit = {
    if (!other.isZero) {
      other.value.foreach(str => {
        myacc.add(str)
      })
    }
  }

  override def value: mutable.HashSet[String] = {
    myacc
  }
}
