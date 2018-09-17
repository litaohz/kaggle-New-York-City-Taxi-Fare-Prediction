package com.netease.music.algo.event.model

import java.util.Calendar
import Path._
import com.netease.music.algo.event.compute.SnsBase.getNdaysShift
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
  * litao, 2018/05/25
  * 从一定时间日志中获取正负样本
  */
object CollectSample {

  case class EventSample(userId: Long, eventId: Long, creatorId: String, logTime: Long, alg: String)

  case class PreLog(contentType: String, userId: Long, eventId: Long, action: Int, logTime: Long)

  def timeToDay = udf((logTime: Long) => {
    val cal = Calendar.getInstance
    cal.setTimeInMillis(logTime)
    cal.get(Calendar.DAY_OF_YEAR)
  })



  def monthDay = udf((logTime: Long) => {
    val cal = Calendar.getInstance
    cal.setTimeInMillis(logTime)
    cal.get(Calendar.DAY_OF_MONTH)
  })


  def markLabel = udf((sumAction: Int) => if (sumAction > 0) 1 else 0)

  def getFirstItem = udf((algs: Seq[String]) => if (!algs.isEmpty) algs.head else "")


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._


    val preLogInput = getImpressPath(args(0))
    val followPath = getFollowPath(args(0))
    spark.read.parquet(followPath).createOrReplaceTempView("follow")
//     确定要取的日期范围

    spark.sparkContext.textFile(preLogInput).map(_.split("\t")).
      filter(line => (line(1) == "eventclick" || line(1) == "eventimpress") &&
        !line(4).toLowerCase.contains("featured") && !line(4).toLowerCase.contains("mock") && !line(4).equalsIgnoreCase("alg") &&
        !line(0).toLowerCase.equals("eventactivity") && !line(4).toLowerCase.contains("qrt") &&
        !line(5).equals("0") && !line(5).isEmpty).map(array => {
      EventSample(array(2).toLong, array(3).toLong, array(5), array(6).toLong, array(4))
    }).toDF.withColumn("logDay", timeToDay($"logTime")).createOrReplaceTempView("eventInfo")

    spark.sql("select  a.userid, a.eventid, a.logDay," +
      "case when (b.userid is not null and b.eventid is not null) then 1 else 0 end as label  from " +
      " (select userid, eventid, creatorid, logDay from eventInfo )a left outer join " +
      " (select userid, props['id'] as eventid from follow where props['actionType'] in" +
      " ('intoPersonalPage','follow','comment'))b on a.userid = b.userid and a.eventid = b.eventid ").
      groupBy($"userId", $"eventId", $"logDay").
      agg(max($"label").alias("label")).
      write.mode(SaveMode.Overwrite).parquet(sampleInput.concat("ds=").concat(args(0)))

  }

  def getImpressPath(ds: String): String = {
    val sb = new StringBuilder

    val baseStr = "/user/ndir/music_recommend/event/mainpage_log/"

    for (n <- 0 to 0) {
      sb.append(baseStr).append(getNdaysShift(ds, n)).append("/event,")
    }
    sb.dropRight(1).toString()
  }


  def getFollowPath(ds: String): String = {
    "/user/da_music/hive/warehouse/music_dw.db/user_action/dt=".concat(ds).concat("/action=eventclick")

  }

  }
