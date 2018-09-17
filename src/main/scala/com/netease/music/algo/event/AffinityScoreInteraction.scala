package com.netease.music.algo.event

import com.netease.music.algo.event.compute.Sns1._
import com.netease.music.algo.event.path.OutputPath._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/*
todo: 处理下c_score为null的情况
 */
object AffinityScoreInteraction {

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  val hc = new org.apache.spark.sql.hive.HiveContext(spark.sparkContext)


  def main(args: Array[String]): Unit = {
    spark.read.parquet(interactionPath1).createOrReplaceTempView("interaction")
    spark.udf.register("i_score", iScore)
    val interaction = spark.sql(makeSql(args))
    spark.read.parquet(outputPathATU).join(interaction,Seq("userid", "friendid"),"left_outer")
      .na.fill(0, Seq("cnt", "cnt4", "cnt7", "cnt15","cnt31"))
      .write.mode(SaveMode.Overwrite).parquet(outputPathATI.concat(args(0)))

  }

  def makeSql(args: Array[String]): String = {
    val s1 = "(select user_id as userid, cretorid as friendid, cnt, 0 as cnt4, 0 as cnt7, 0 as cnt15, 0 as cnt31 from interaction" +
      " where datediff(ds,date_add('" + args(0) + "', 0)) >= 0  union all "

    val s4 = "select user_id as userid, cretorid as friendid, 0 as cnt, sum(cnt) as cnt4, 0 as cnt7, 0 as cnt15, 0 as cnt31  from interaction" +
      " where datediff(ds,date_add('" + args(0) + "', -3)) >= 0  group by user_id, cretorid  union all "
    val s7 = "select user_id as userid, cretorid as friendid,0 as cnt , 0 as cnt4, sum(cnt) as  cnt7 , 0 as cnt15, 0 as cnt31  from interaction" +
      " where datediff(ds,date_add('" + args(0) + "', -6)) >= 0  group by user_id, cretorid  union all "

    val s15 = "select user_id as userid, cretorid as friendid, 0 as cnt,  0 as cnt4, 0 as cnt7, sum(cnt) as  cnt15 , 0 as cnt31  from interaction" +
      " where datediff(ds,date_add('" + args(0) + "', -14)) >= 0  group by user_id, cretorid  union all "

    val s31 = "select user_id as userid, cretorid as friendid, 0 as cnt, 0 as cnt4, 0 as cnt7, 0 as cnt15,  sum(cnt) as   cnt31  from interaction" +
      " where datediff(ds,date_add('" + args(0) + "', -30)) >= 0  group by user_id, cretorid )t"
    "select userid, friendid, sum(cnt) as cnt, sum(cnt4) as cnt4, sum(cnt7) as cnt7, sum(cnt15) as cnt15, sum(cnt31) as cnt31 from ".
      concat(s1).concat(s4).concat(s7).concat(s15).concat(s31).concat(" group by userid, friendid")
  }

  val iScore = ((cnt: Long) => {
    1 + Math.log10(cnt + 1)
  })
}
