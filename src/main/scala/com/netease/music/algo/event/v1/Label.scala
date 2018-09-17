package com.netease.music.algo.event.v1

import com.netease.music.algo.event.compute.SnsBase.getNdaysShift
import com.netease.music.algo.event.dim.Path.S2PPath
import com.netease.music.algo.event.path.DeletePath.delete
import com.netease.music.algo.event.schema.BaseSchema._
import com.netease.music.algo.event.udf.Udf1._
import com.netease.music.algo.event.v1.Path._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{explode, sum}
import org.apache.spark.sql.{SaveMode, SparkSession}

/*


 */
object Label {

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    delete(spark, labelPre.concat("ds=").concat(getNdaysShift(args(0),-30)))
/*
spark.sql("select userid, props['alg'],props['targetid'] as puid from data
 where props['page'] = 'eventpage' and action in
 ('impress','click') and props['target'] in
 ('rcmmd_user','singer','taste','acquaintance','publisher','user_card','not_interested','follow','userphoto') ")
 */
    val data = spark.read.parquet("/user/da_music/hive/warehouse/music_dw.db/user_action_fast/dt=".concat(args(0)))
    data.createOrReplaceTempView("data")
    spark.sql("select userid,case when props['target'] = 'follow' then props['resourceid'] else props['targetid'] end as puid," +
      "case when action = 'click' and props['target'] in ('not_interested') " +
      "and props['resource'] = 'rcmmd_user' and props['page'] = 'eventpage' then 1 else 0 end as dislike, " +

      "case when action = 'impress' and props['target'] in ('user_card') and " +
      "props['resource'] in ('rcmmd_user','singer','taste','acquaintance','publisher') " +
      "and props['page'] = 'eventpage' then 1 else 0 end as impress," +

      "case when action = 'click' and props['target'] in ('userphoto') and props['resource'] in " +
      "('rcmmd_user','singer','taste','acquaintance','publisher') and props['page'] = 'eventpage' then 1 else 0 end as click_photo," +

      "case when action = 'click' and props['target'] in ('follow') " +
        "and props['targetid'] = 'button' and props['resource'] in ('user_card') and " +
      "props['type'] in ('rcmmd_user','singer','taste','acquaintance','publisher') and " +
      "props['page'] = 'eventpage' then 1 else 0 end as follow" +

      " from data where  " +
      " props['page'] = 'eventpage'" +
      "    and action in ('impress','click')" +
      "and props['target'] in ('rcmmd_user','singer','taste','acquaintance','publisher','user_card','not_interested','follow','userphoto')"
    ).where("puid is not null and puid !='module'").groupBy("userid","puid").
      agg(sum($"dislike").as("dislike"),sum($"impress").as("impress"),sum($"click_photo").as("click_photo"), sum($"follow").as("follow"))
    .write.mode(SaveMode.Overwrite).parquet(labelPre.concat("ds=").concat(args(0)))

  }


}
