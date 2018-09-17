package com.netease.music.algo.event.backup

import com.netease.music.algo.event.path.OutputPath._
import com.netease.music.algo.event.schema.BaseSchema._
import com.netease.music.algo.event.udf.ComputeCscoreAgg
import com.netease.music.algo.event.udf.ComputeScore._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/*
被关注着黑名单
9003
1
48353

+---------+
|max(size)|
+---------+
|    21969|
+---------+

res34.count
res36: Long = 721620
第一版上线ready：20180221
 */
object MutualBak {

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  import spark.implicits._
  val evt = spark.read.option("sep", "\t").schema(tranEventSchema).csv("music_recommend/event/event_meta/current_event_meta")
  def main(args: Array[String]): Unit = {

    val mutual = evt.withColumn("m_score",computeMscore($"zanTimeStr",$"commentTimeStr",$"forwardTimeStr")).
      select($"creatorid",$"m_score",$"createTime").withColumn("size", computeSize($"m_score")).where("size>0").
      withColumn("m_score", transMapScore($"m_score")).withColumn("u_score", computeUscore(3)($"createTime"))
    mutual.createOrReplaceTempView("mutual")
    spark.udf.register("ComputeCscoreAgg", new ComputeCscoreAgg)

    val result = spark.sql("select creatorid,  ComputeCscoreAgg(m_score) as m_score ,sum(size) as size, max(u_score) as u_score  from mutual group by creatorid ")
    result.write.mode(SaveMode.Overwrite).parquet(mutualPathBak)
  }
}
