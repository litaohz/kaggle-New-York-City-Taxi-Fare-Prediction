package com.netease.music.algo.event.analyse

import com.netease.music.algo.event.path.OutputPath._
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{SaveMode, SparkSession}

/*
 spark.sql("select *,cast (props['time'] as bigint) as duration from view\n" +
      "where props['type']='end'\n" +
      "and props['time'] between 0 and 18000 \n" +
      "and props['id'] in ('NMFriendsViewController','tracktimeline')")
 */
object DurationN {
  def main(args: Array[String]): Unit = {
    val view = spark.read.parquet(durationPath1)
    view.createOrReplaceTempView("duration")
    val durationN = spark.sql(makeSql(args))
    durationN.createOrReplaceTempView("durationN")
    val avg_30 = spark.sql("select sum(duration30)/count(1) as avg_30 from durationN ").first().getAs[Double](0)
    spark.sql("select  userid,  duration,  duration4,  duration7, duration15,  duration30, " + avg_30 + " as avg_30 " +
      "from durationN").write.mode(SaveMode.Overwrite).parquet(durationPathOutput.concat(args(0)))

  }

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf).enableHiveSupport()
    .getOrCreate()

  def makeSql(args: Array[String]): String = {
    val s1 = "(select userid,  duration, 0 as duration4, 0 as duration7, 0 as duration15, 0 as duration30 from duration" +
      " where datediff(ds,date_add('" + args(0) + "', 0)) >= 0  union all "

    val s4 = "select userid,  0 as duration, sum(duration) as duration4, 0 as duration7, 0 as duration15, 0 as duration30  from duration" +
      " where datediff(ds,date_add('" + args(0) + "', -3)) >= 0  group by userid  union all "
    val s7 = "select userid, 0 as duration , 0 as duration4, sum(duration) as  duration7 , 0 as duration15, 0 as duration30  from duration" +
      " where datediff(ds,date_add('" + args(0) + "', -6)) >= 0  group by userid  union all "

    val s15 = "select userid,  0 as duration,  0 as duration4, 0 as duration7, sum(duration) as  duration15 , 0 as duration30  from duration" +
      " where datediff(ds,date_add('" + args(0) + "', -14)) >= 0  group by userid  union all "

    val s31 = "select userid,  0 as duration, 0 as duration4, 0 as duration7, 0 as duration15,  sum(duration) as   duration30  from duration" +
      " where datediff(ds,date_add('" + args(0) + "', -29)) >= 0  group by userid )t"
    "select userid, sum(duration) as duration, sum(duration4) as duration4, sum(duration7) as duration7, sum(duration15) as duration15, sum(duration30) as duration30 from ".
      concat(s1).concat(s4).concat(s7).concat(s15).concat(s31).concat(" group by userid")
  }
}
