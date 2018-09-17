package com.netease.music.algo.event.contentbase.reason

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{concat_ws, explode}
import Path._
/*
nkv key   :
videoId-vType_V_I_P_N
nkv value:
vType  \t  creatorId  \t  artistIds  \t  videoBgmIds  \t
bgmIds  \t  userTagIds  \t  auditTagIds  \t  category  \t  isControversial  \t
rawPrediction  \t  rawPrediction4newUser  \t  isMusicVideo  \t  downgradeScore  \t  poolStatsScore
 */
object Pool {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._
    spark.read.parquet("music_recommend/event/getEventResourceInfoForEventPool_eventInfo/parquet").createOrReplaceTempView("pool")
    spark.sql("select eventId,'event' as vType, creatorId,artistIdsForRec as artistIds, songIdsForRec as videoBgmIds," +
      "'0' as bgmIds,'0' as userTagIds,'0' as auditTagIds,eventCategory as category,'0' as isControversial," +
      "'0' as rawPrediction,'0' as rawPrediction4newUser,isMusicEvent as isMusicVideo,'0' as downgradeScore,'0' as poolStatsScore" +
      " from pool").
      select(concat_ws("\t", $"eventId", $"vType",$"creatorId",$"artistIds",$"videoBgmIds",
        $"bgmIds",$"userTagIds",$"auditTagIds",$"category",$"isControversial",$"rawPrediction",
        $"rawPrediction4newUser",$"isMusicVideo",$"downgradeScore",$"poolStatsScore")).repartition(5).
    write.mode(SaveMode.Overwrite).text(reasonPath)


  }

}
