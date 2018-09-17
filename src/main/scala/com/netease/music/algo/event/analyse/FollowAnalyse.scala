package com.netease.music.algo.event.analyse

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object FollowAnalyse {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._
    val schema = StructType(Array(StructField("id", StringType, nullable = false), StructField("userid", StringType, nullable = false),
      StructField("friendid", StringType, nullable = false), StructField("createtime", StringType, nullable = false), StructField("mutual", BooleanType, nullable = false)))
    val follow = spark.read.option("sep", "\t").schema(schema).csv("/db_dump/music_ndir/Music_Follow")
    follow.createOrReplaceTempView("follow")
    //    val ana = spark.sql("select count (1) from follow")
    //    ana.show(100)
    //    val ana1 = spark.sql("select count (distinct userid) from follow")
    //    ana1.show(100)
    //    val ana2 = spark.sql("select count (distinct friendid) from follow")
    //    ana2.show(100)
        val ana3 = spark.sql("select userid,count(1) as count from follow group by userid")
    //
//        ana3.select("userid", "count").orderBy($"count".desc).show(100)
    val ana4 = spark.sql("select friendid,count(1) as count from follow group by friendid")
    //
    //    val count = ana4.select("friendid", "count").where("count > 50000").count()
    //    println(count)
    //    val ana5 = spark.sql("select count(userid) as count from follow where mutual=True")

    //    ana5.show(100)

        ana3.createOrReplaceTempView("follow3")
        val ana6 = spark.sql("select (case when count > 1 and count < 10 then \"group1\" " +
          "when count >= 10 and count < 20 then \"group2\" " +
          "when count >= 20 and count < 40 then \"group3\" " +
          "when count >= 40 and count < 70 then \"group4\" " +
          "when count >= 70 and count < 100 then \"group5\" " +
          "when count >= 100 and count < 150 then \"group6\" " +
          "when count >= 150 and count < 300 then \"group7\" " +
          "when count >= 300 and count < 1000 then \"group8\" " +
          "when count >= 1000 and count < 2000 then \"group9\" " +
          "when count >= 2000 and count < 3000 then \"group10\" " +
          "else  \"group11\" end) as group_user  from follow3 where count > 1")
    ana6.createOrReplaceTempView("follow4")
    val ana9 = spark.sql("select group_user, count(1) from follow4 group by group_user")
    ana9.show(100)
    ana4.createOrReplaceTempView("follow1")
    val ana7 = spark.sql("select (case when count >= 1 and count < 100 then \"group1\" " +
      "when count >= 100 and count < 500 then \"group2\" " +
      "when count >= 500 and count < 2000 then \"group3\" " +
      "when count >= 2000 and count < 5000 then \"group4\" " +
      "when count >= 5000 and count < 10000 then \"group5\" " +
      "when count >= 10000 and count < 20000 then \"group6\" " +
      "when count >= 20000 and count < 50000 then \"group7\" " +
      "when count >= 50000 and count < 80000 then \"group8\" " +
      "when count >= 80000 and count < 200000 then \"group9\" " +
      "when count >= 200000 and count < 500000 then \"group10\" " +
      "else  \"group11\" end) as group_friend  from follow1")
    ana7.createOrReplaceTempView("follow2")
    val ana8 = spark.sql("select group_friend, count(1) from follow2 group by group_friend")
    ana8.show(100)
  }
}
