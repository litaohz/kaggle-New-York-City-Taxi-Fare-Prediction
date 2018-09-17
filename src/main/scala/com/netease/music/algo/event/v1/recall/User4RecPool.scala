package com.netease.music.algo.event.v1.recall

import com.netease.music.algo.event.schema.BaseSchema._
import com.netease.music.algo.event.udf.Udf1._
import com.netease.music.algo.event.v1.Functions.matchString
import com.netease.music.algo.event.v1.Path._
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object User4RecPool {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate


    import spark.implicits._

    spark.read.parquet(authDimenson).createOrReplaceTempView("auth")

    spark.read.parquet("/user/da_music/hive/warehouse/music_dw.db/cloudmusic_official_account").
      createOrReplaceTempView("official")
   spark.sql("select a.userid from (select userid from auth)a " +
      " left outer join(select userid from official) b on a.userid = b.userid  " +
      " where b.userid is null " ).repartition(1).
      write.mode(SaveMode.Overwrite).parquet(userRecPool)

  }





}
