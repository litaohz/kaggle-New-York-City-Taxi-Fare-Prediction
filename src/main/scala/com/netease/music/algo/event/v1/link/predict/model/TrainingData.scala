package com.netease.music.algo.event.v1.link.predict.model

import com.netease.music.algo.event.dim.Path._
import com.netease.music.algo.event.schema.BaseSchema._
import com.netease.music.algo.event.udf.Udf1._
import com.netease.music.algo.event.v1.Path._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.netease.music.algo.event.model.Udf._
/*


 */
object TrainingData {

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    import spark.implicits._

    spark.read.parquet(labelPre).where("follow >0").
      union(spark.read.parquet(labelPre).where("follow >0 and click_photo >0")).
    union(spark.read.parquet(labelPre).where("follow =0 and click_photo =0").where(testUserid(23)($"userid"))).
      createOrReplaceTempView("data")
    spark.sql("select *, case when follow >0 or click_photo >0 then 1 else 0 end as label from data ").
      createOrReplaceTempView("label")



    spark.read.option("sep", "\t").
      schema(commonSchema).csv("music_recommend/feed_video/userProfile/searchProfile_days").
      select("userid", "result").withColumn("result", explode(splitResult(($"result")))).
      withColumn("songid", extractResult(0)($"result")).drop("result").createOrReplaceTempView("searched")

    spark.read.option("sep", "\t").
      schema(commonSchema7).csv("music_recommend/user_local_song").createOrReplaceTempView("localed")
    spark.sql("select a.userid,a.puid,a.label, b.songid as song_s, c.friendid as song_l from (select * from label)a " +
      "left outer join (select * from searched)b on a.userid = b.userid " +
      "left outer join (select * from localed)c on a.userid = c.userid ").createOrReplaceTempView("label_1")
    spark.read.parquet(S2PPathScatter).createOrReplaceTempView("s2p")
    spark.sql("select a.userid,a.puid,a.dislike,a.impress,a.click_photo,a.follow, " +
      "case when (a.song_s is not null and b.songid is not null and b.userid = a.puid and b.songid = a.song_s) " +
      "then  1  else 0 end as from_search, " +
      "case when (a.song_l is not null and b.songid is not null and b.userid = a.puid and b.songid = a.song_l) " +
      "then  1  else 0 end as from_local " +
      "from (select * from label_pre)a left outer join  " +
      "from (select * from s2p)b on (a.song_s = b. songid or a.song_l = b.songid)  ").
      write.mode(SaveMode.Overwrite).parquet(trainingData)

  }


}
