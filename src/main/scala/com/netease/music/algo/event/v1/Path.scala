package com.netease.music.algo.event.v1

object Path {
  val puidCandidates = "music_recommend/event/sns/v1/candidates/puid/"
  val followCfPre = "music_recommend/event/sns/v1/followbased/cf/pre"
  val followCfPre1 = "music_recommend/event/sns/v1/followbased/cf/pre1"
  val authDim = "music_recommend/event/sns/v1/followbased/dim/auth"
  val u2aDim = "music_recommend/event/sns/v1/dim/u2a"
  val u2NameDim = "music_recommend/event/sns/v1/dim/u2name"
  val followCfRes = "music_recommend/event/sns/v1/followbased/cf/result"
  val followCfResLine = "music_recommend/event/sns/v1/followbased/cf/res_line"
  val followMergeResLine = "music_recommend/event/sns/v1/followbased/merge/res_line"
  val followN2VResLine = "music_recommend/event/sns/v1/followbased/node2vec/res_line/predict.txt"
  val searchCandidates = "music_recommend/event/sns/v1/candidates/search/"
  val labelPre = "music_recommend/event/sns/v1/label/pre/"
  val labelPre1 = "music_recommend/event/sns/v1/label/pre1/"
  val trainingData = "music_recommend/event/sns/v1/training/data/"
  val modelOutput = "music_recommend/event/sns/v1/training/model/"
  val modelOutputAls = "/user/ndir/music_recommend/feed_video/follow/als_direct_follow_model.all/model2"


  val organicRecall = "/user/ndir/music_recommend/user_action/user_profile/"
  val organicRecall1 = "/user/ndir/music_recommend/event/user_play_song_artistId_long/"

  val eventPoolPath = "music_recommend/event/getEventResourceInfoForEventPool_eventInfo/parquet"
  val songDim = "/user/da_music/hive/warehouse/music_dimension.db/song_meta_info_history/dt="
  val authDimenson = "/user/da_music/hive/warehouse/music_db_front.db/music_authenticateduser"


  val tmpS2u = "music_recommend/event/sns/v1/recall/tmp/s2u"
  val tmpFollowAuth = "music_recommend/event/sns/v1/recall/tmp/follow_auth"
  val matchResult = "/user/ndir/music_recommend/event/creator_match_result/"
  val userRecPool = "/user/ndir/music_recommend/event/creator_pool/"
  val matchResultAll = "/user/ndir/music_recommend/event/creator_match_result_all/"
  val matchResultForEvent = "music_recommend/event/rec/rec_followed_sim/"
  val matchResultForEventTmp = "music_recommend/event/rec/rec_followed_sim_tmp/"
  val impressAddFollow = "music_recommend/event/sns/addfollow/analyse/impress/"
  val impressAddFollow1 = "music_recommend/event/sns/addfollow/analyse/impress/ds="

}
