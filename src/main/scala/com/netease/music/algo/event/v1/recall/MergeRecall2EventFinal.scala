package com.netease.music.algo.event.v1.recall

import com.netease.music.algo.event.schema.BaseSchema._
import com.netease.music.algo.event.udf.Udf1._
import com.netease.music.algo.event.v1.Path._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{collect_list, concat_ws, explode, udf}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object MergeRecall2EventFinal {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate


    import spark.implicits._

    var reced = spark.read.option("sep", "\t").schema(commonSchema).csv("/user/ndir/music_recommend/event/event_impress_all").
      withColumnRenamed("result","filter_evt")

    val merge1 = spark.read.option("sep", "\t").schema(commonSchema).csv(matchResultForEventTmp)

   merge1.join(reced,"userid").withColumn("result",filterResult($"result",$"filter_evt")).
      select(concat_ws("\t", $"userid", $"result"))
     .write.option("compression", "gzip").mode(SaveMode.Overwrite).text(matchResultForEvent)
  }

  def filterResult: UserDefinedFunction = udf((result:String, filterStr:String) => {
    val filtered = filterStr.split(",")
    val eventInfo = result.split(",").filter(x=>(!filtered.contains(x)))
    val len = if (eventInfo.length > 50) eventInfo.length - 50 else 0
    val res = eventInfo.dropRight(len).map(x=>x.concat("_0_0")).mkString(",")
    res
  })

}
