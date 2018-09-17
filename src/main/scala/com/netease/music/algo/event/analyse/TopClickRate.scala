package com.netease.music.algo.event.analyse

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{concat_ws, udf}
import com.netease.music.algo.event.path.OutputPath._
import com.netease.music.algo.event.top.features.ReOrder.spark
import com.netease.music.algo.event.udf.UserActionUdf._

object TopClickRate {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    spark.read.parquet(clkRatePath.concat(args(0))).createOrReplaceTempView("clk")
    spark.sql("select sum(impress) as impress, sum(click) as click from clk").withColumn("clkRate", $"click" / $"impress").
      select(concat_ws("\t", $"impress", $"click", $"clkRate")).
      write.mode(SaveMode.Overwrite).text(analyseOutput.concat(args(0)))

    spark.sql("select alg, sum(impress) as impress, sum(click) as click from clk group by alg").
      withColumn("clkRate", $"click" / $"impress").
      select(concat_ws("\t", $"impress", $"click", $"clkRate", $"alg")).
      write.mode(SaveMode.Overwrite).text(analyseOutputDetail.concat(args(0)))
  }

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf).enableHiveSupport()
    .getOrCreate()

}
