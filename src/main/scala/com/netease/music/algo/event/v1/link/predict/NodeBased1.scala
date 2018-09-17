package com.netease.music.algo.event.v1.link.predict

import com.netease.music.algo.event.Rank.FriendIds
import com.netease.music.algo.event.schema.BaseSchema._
import com.netease.music.algo.event.v1.Path._
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{collect_list, concat_ws, row_number, udf}
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.netease.music.algo.event.udf.Udf1._

object NodeBased1 {
  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf)

    .getOrCreate()

  def main(args: Array[String]): Unit = {
    spark.read.option("sep", "\t").schema(followSchema).csv("/db_dump/music_ndir/Music_Follow").
      filter("friendid not in(1,9003,48353,1305093793)").createOrReplaceTempView("follow")
    spark.read.parquet("/user/da_music/hive/warehouse/music_db_front.db/music_authenticateduser").createOrReplaceTempView("auth")
    spark.sql("select a.userid,a.friendid from (select * from follow) a " +
      " join (select * from auth)b on a.friendid=b.userid").write.mode(SaveMode.Overwrite).parquet(tmpFollowAuth)


    import spark.implicits._
    val windowSpec = Window.partitionBy("userId").orderBy($"score".desc)
    val cf1 = spark.read.option("sep", "\t").schema(commonSchema7).csv(followCfRes).
      select($"userId", row_number().over(windowSpec).as("rn"), $"friendid").rdd.
      map(row => (row.getString(0), FriendIds(row.getInt(1), row.getString(2)))).
      groupByKey.
      map({ case (key, value) =>
        (key, value.toArray
          .sortWith(_.rank < _.rank)
          .map(_.friendid))
      }).toDF("userid", "result").withColumn("result", cutArrayAndToString("cf1:")($"result"))

    cf1.select(concat_ws("\t", $"userid", $"result")).repartition(5).
      write.mode(SaveMode.Overwrite).text(followCfResLine)


    val cf2 = spark.read.option("sep", "\t").schema(commonSchema).
      csv(followN2VResLine)
    cf1.union(cf2).
      groupBy("userId").agg(collect_list($"result").alias("result")).
      select($"userId", concat_ws(";", $"result").alias("result")).
      select(concat_ws("\t", $"userid", $"result")).repartition(5).
      write.mode(SaveMode.Overwrite).text(followMergeResLine)


  }


}
