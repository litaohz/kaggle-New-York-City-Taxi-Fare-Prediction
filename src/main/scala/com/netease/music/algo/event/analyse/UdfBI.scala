package com.netease.music.algo.event.analyse

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object UdfBI {

  def countArray: UserDefinedFunction = udf((eventInfo: String) => {
    eventInfo.split(",").length < 8

  })

  def check: UserDefinedFunction = udf((result: String) => {
    result.contains("s")

  })
}
