package com.netease.music.algo.event.analyse

import com.netease.music.algo.event.path.OutputPath._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.concat_ws
import org.apache.spark.sql.{SaveMode, SparkSession}

object TopClickUVRate {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    spark.read.parquet(clkRatePath.concat(args(0))).createOrReplaceTempView("clk")

    spark.sql("select count(distinct userid) as impress, count ( distinct case when click >0 then userid else null end) as click from clk").
      withColumn("clkRate", $"click" / $"impress").
      select(concat_ws("\t", $"impress", $"click", $"clkRate")).
      write.mode(SaveMode.Overwrite).text(analyseOutputUv.concat(args(0)))

    spark.sql("select alg,count(distinct userid) as impress, " +
      "count ( distinct case when click >0 then userid else null end) as click from clk group by alg").
      withColumn("clkRate", $"click" / $"impress").
      select(concat_ws("\t", $"impress", $"click", $"clkRate" , $"alg")).
      write.mode(SaveMode.Overwrite).text(analyseOutputUvDetail.concat(args(0)))
  }

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf).enableHiveSupport()
    .getOrCreate()

}
