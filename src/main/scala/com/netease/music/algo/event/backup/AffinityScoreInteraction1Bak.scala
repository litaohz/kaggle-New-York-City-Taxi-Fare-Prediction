package com.netease.music.algo.event.backup

import com.netease.music.algo.event.path.OutputPath._
import com.netease.music.algo.event.udf.ComputeScore._
import org.apache.spark.SparkConf
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{SaveMode, SparkSession}

/*
scala> res29.count
res31: Long = 121885035

res29.where("cnt15 > 0").count
res32: Long = 62252460

res29.where("cnt7 > 0").count
res33: Long = 32396881

res29.where("cnt4 > 0").count
res34: Long = 17208324

res29.where("cnt > 0").count
res35: Long = 4790839



scala> res15.count
res17: Long = 347495930

scala> res15.where("score > 1").count
res18: Long = 39368406

scala> res15.where("score > 10").count
res19: Long = 424336

 */
object AffinityScoreInteraction1Bak {

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  val hc = new org.apache.spark.sql.hive.HiveContext(spark.sparkContext)

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val pipeline = PipelineModel.load(modelOutput)
    pipeline.transform(spark.read.parquet(outputPathATI.concat(args(0)))).
      withColumn("i_score", vectorHead($"probability")).createOrReplaceTempView("t_i_score")
    spark.read.parquet(outputPathATN).createOrReplaceTempView("t_n_score")
    spark.sql("select a.userid,a.friendid, " +
      "case when (b.userid is not null and b.friendid is not null)then (a.n_score + b.i_score) else a.n_score end as score" +
      " from " +
      "(select userid,friendid,n_score from t_n_score)a left outer join (select userid,friendid,i_score from t_i_score)b on" +
      " a.userid=b.userid and a.friendid=b.friendid").
      write.mode(SaveMode.Overwrite).parquet(outputPathAT1)

  }
}
