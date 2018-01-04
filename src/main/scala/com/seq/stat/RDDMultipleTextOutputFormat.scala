package com.seq.stat

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.io.NullWritable

class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateActualKey(key: Any, value: Any): Any =
    NullWritable.get()

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    val valueTuple3 = value.asInstanceOf[Tuple3[String, String, String]]
    val parent = valueTuple3._1
    val fileName = valueTuple3._2
    String.format("%s/Group/%s/%s", parent, key.asInstanceOf[String], fileName)
  }

  override def generateActualValue(key: Any, value: Any): String = {
    val valueTuple3 = value.asInstanceOf[Tuple3[String, String, String]]
    valueTuple3._3
  }

}
