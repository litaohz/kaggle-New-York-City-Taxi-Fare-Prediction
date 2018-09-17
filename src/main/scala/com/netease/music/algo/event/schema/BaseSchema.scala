package com.netease.music.algo.event.schema

import org.apache.spark.sql.types._

object BaseSchema {

  val commonSchema = StructType(Array(StructField("userid", StringType, nullable = false), StructField("result", StringType, nullable = false)))
  val commonSchema1 = StructType(Array(StructField("userid", StringType, nullable = false)))
  val commonSchema2 = StructType(Array(StructField("eventid", StringType, nullable = false)))
  val commonSchema3 = StructType(Array(StructField("songid", StringType, nullable = false), StructField("artists_info", StringType, nullable = false)))
  val commonSchema4 = StructType(Array(StructField("userid", StringType, nullable = false), StructField("songinfo", StringType, nullable = false)))
  val commonSchema5 = StructType(Array(StructField("userid", StringType, nullable = false), StructField("friendids", StringType, nullable = false)))
  val commonSchema6 = StructType(Array(StructField("userid", StringType, nullable = false), StructField("friend1", StringType, nullable = false), StructField("friendids", StringType, nullable = false)))
  val commonSchema7 = StructType(Array(StructField("userid", StringType, nullable = false), StructField("friendid", StringType, nullable = false), StructField("score", DoubleType, nullable = false)))
  val commonSchema7_1 = StructType(Array(StructField("userIdStr", LongType, nullable = false), StructField("friendIdStr", LongType, nullable = false), StructField("rating", DoubleType, nullable = false)))
  val commonSchema8 = StructType(Array(StructField("artistid", StringType, nullable = false), StructField("result", StringType, nullable = false)))

  val followSchema = StructType(Array(StructField("id", StringType, nullable = false), StructField("userid", StringType, nullable = false),
    StructField("friendid", StringType, nullable = false), StructField("createtime", StringType, nullable = false), StructField("mutual", BooleanType, nullable = false)))

  val schemaPhoneFriend = StructType(Array(StructField("userid", StringType, nullable = false),
    StructField("friendid", StringType, nullable = false), StructField("jointime", StringType, nullable = false), StructField("json", StringType, nullable = true)))


  val schemaStar = StructType(Array(StructField("userid", StringType, nullable = false),
    StructField("db_update", StringType, nullable = false), StructField("dt_create", StringType, nullable = false)))
  val artistSchema = StructType(Array(StructField("userid", LongType, nullable = false),
    StructField("artistid", StringType, nullable = true), StructField("artistname", StringType, nullable = true), StructField("tag", StringType, nullable = true),
    StructField("status", IntegerType, nullable = true)))


  val videoSchema = StructType(Array(
    StructField("impress", StringType, nullable = true),
    StructField("play", StringType, nullable = false),
    StructField("os", StringType, nullable = false),
    StructField("userid", LongType, nullable = true),
    StructField("timestamp", StringType, nullable = false),
    StructField("videoid", StringType, nullable = false)
  ))




  val tranEventSchema = StructType(
    Array(
      StructField("eventId", LongType, nullable = false),
      StructField("resourceTypeNid", StringType, nullable = false),
      StructField("creatorId", StringType, nullable = false),
      StructField("createTime", LongType, nullable = false),
      StructField("zanNum", LongType, nullable = false),
      StructField("commentNum", LongType, nullable = false),
      StructField("forwardNum", LongType, nullable = false),
      StructField("commentClickNum", LongType, nullable = false),
      StructField("zanTimeStr", StringType, nullable = false),
      StructField("commentTimeStr", StringType, nullable = false),
      StructField("forwardTimeStr", StringType, nullable = false),
      StructField("commentClickTimeStr", StringType, nullable = false),
      StructField("resPlayNum", LongType, nullable = false),
      StructField("photoClickNum", LongType, nullable = false),
      StructField("intoDetailNum", LongType, nullable = false),
      StructField("resPlayTimeStr", StringType, nullable = false),
      StructField("photoClickTimeStr", StringType, nullable = false),
      StructField("intoDetailTimeStr", StringType, nullable = false),
      StructField("intoEventactivityNum", LongType, nullable = false),
      StructField("intoPersonNum", LongType, nullable = false),
      StructField("followNum", LongType, nullable = false),
      StructField("intoEventactivityTimeStr", StringType, nullable = false),
      StructField("intoPersonTimeStr", StringType, nullable = false),
      StructField("followTimeStr", StringType, nullable = false),
      StructField("isVideoEvent", IntegerType, nullable = false),
      StructField("isResourceTypeSpecial", IntegerType, nullable = false)
    )
  )
  /*
  contentType + "\t" + action + "\t" + user_id + "\t" + id + "\t" + alg + "\t" +
                     sourceid + "\t" + logTime + "\t" + os + "\t" + actionType + "\t" + position
   */
  val clickLogSchema = StructType(
    Array(
      StructField("content_type", StringType, nullable = false),
      StructField("action", StringType, nullable = false),
      StructField("user_id", LongType, nullable = false),
      StructField("evt_id", LongType, nullable = false),
      StructField("alg", StringType, nullable = false),
      StructField("cretorid", StringType, nullable = false),
      StructField("timestamp", LongType, nullable = false),
      StructField("os", StringType, nullable = false),
      StructField("action_type", StringType, nullable = false),
      StructField("position", LongType, nullable = false)

    ))

  val whiteListSchema = StructType(
    Array(
      StructField("evt_id", StringType, nullable = true),
      StructField("res_type", StringType, nullable = true),
      StructField("res_id", StringType, nullable = true),
      StructField("create_user_id", StringType, nullable = true),
      StructField("evt_create_time", StringType, nullable = true),
      StructField("containtype", StringType, nullable = true),
      StructField("evt_len", StringType, nullable = true),
      StructField("pics_num", StringType, nullable = true),
      StructField("eng_ch_ratio", StringType, nullable = true),
      StructField("art_user", StringType, nullable = true),
      StructField("text_quality_score", StringType, nullable = true),
      StructField("is_gif", StringType, nullable = true),
      StructField("topic_id", StringType, nullable = true),
      StructField("is_white_user_tag", StringType, nullable = true),
      StructField("punct_num", StringType, nullable = true),
      StructField("is_timeliness", StringType, nullable = true),
      StructField("is_vedio_evt", StringType, nullable = true),
      StructField("is_restype_specia", StringType, nullable = true)

    )

  )


  val verifiedBackendRcmdEventInfoTableSchema = StructType(
    Array(
      StructField("eventId", LongType, nullable = false),
      StructField("userId", LongType, nullable = false),
      StructField("classification", StringType, nullable = false),
      StructField("artistId", LongType, nullable = false),
      StructField("effective", BooleanType, nullable = false),
      StructField("language", IntegerType, nullable = false),
      StructField("excellent", BooleanType, nullable = false),
      StructField("updateTime", StringType, nullable = false),
      StructField("effectiveTime", LongType, nullable = false),
      StructField("songIdsStr", StringType, nullable = false)
    )
  )
  val picSchema = StructType(
    Array(
      StructField("eventId", LongType, nullable = false),
      StructField("userId", LongType, nullable = false),
      StructField("pics_num", LongType, nullable = false)
    )
  )


}
