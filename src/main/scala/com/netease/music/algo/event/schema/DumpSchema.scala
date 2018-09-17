package com.netease.music.algo.event.schema

import org.apache.spark.sql.types._

object DumpSchema {
  val sptUserSchema = StructType(
    Array(StructField("creatorid", StringType, nullable = false),
      StructField("userclassify", StringType, nullable = false),
      StructField("priority", StringType, nullable = false),
      StructField("artistid", StringType, nullable = false),
      StructField("tags", StringType, nullable = false)
    ))
  val sptEvtSchema = StructType(
    Array(StructField("id", StringType, nullable = false),
      StructField("creatorid", StringType, nullable = false),
      StructField("eventid", StringType, nullable = false),
      StructField("eventtime", StringType, nullable = false),
      StructField("rcmdstate", StringType, nullable = false),
      StructField("starttime", StringType, nullable = false),
      StructField("endtime", StringType, nullable = false)
    ))
}
