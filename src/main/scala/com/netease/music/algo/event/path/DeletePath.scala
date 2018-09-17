package com.netease.music.algo.event.path

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

object DeletePath {


  def delete(spark: SparkSession, pathStr: String): Unit = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val path = new Path("hdfs://hz-cluster4/user/ndir/".concat(pathStr))
    if (hdfs.exists(path)) {
      hdfs.delete(path, true)
    }
  }

  def main(args: Array[String]): Unit = {
    val res0 = "310759499-2:v1,112598107-2:v1=1529337600000"
    val friendId = "275779633"
    var result = ""
    val index = res0.indexOf(",")
    if (index > 0 && index < res0.length - 1) {
      result = res0.substring(0, index + 1).concat(friendId).concat(",").concat(res0.substring(index + 1, res0.length))

    }
    else
      result = res0
    println(result)
  }

}
