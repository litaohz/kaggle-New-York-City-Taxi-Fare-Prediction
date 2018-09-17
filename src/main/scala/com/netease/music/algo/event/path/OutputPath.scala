package com.netease.music.algo.event.path

object OutputPath {
  val activePath = "music_recommend/event/sns/creator/active/"
  //  val whiteListPath = "music_recommend/feed_video/pubuser/pubuser_white/"
  val whiteListPath = "/user/ndir/hive_db/ndir_201712_unicom_tmp.db/dump_event_support_user"

  val affinityRank = "music_recommend/event/sns/affinity/rank/"
  val videoPrefPath = "music_recommend/event/sns/video/pref/"
  val eventSnsPath = "music_recommend/event/sns/event/pref/"
  val mutualPath = "music_recommend/event/sns/mutual/output"
  val interactionPath = "music_recommend/event/sns/features/interaction/ds="
  val interactionPath1 = "music_recommend/event/sns/features/interaction"
  val evtSnsOutput = "music_recommend/event/sns/evt/output/"
  val evtSnsOutputOnline = "music_recommend/event/sns/evt/output_online/"

  val durationPath = "music_recommend/moments/sns/analyse/duration/ds="
  val durationPath1 = "music_recommend/moments/sns/analyse/duration"
  val durationPathOutput = "music_recommend/moments/sns/analyse/duration_output/ds="
  val clkRatePath = "music_recommend/moments/sns/features/clk/ds="
  val clkRatePath4 = "music_recommend/moments/sns/features/clk4/ds="

  val affinityRank1 = "music_recommend/event/sns/affinity/rank1/"

  val outputPathAT = "music_recommend/event/sns/affinity/at/"
  val outputPathATU = "music_recommend/event/sns/affinity/atu/"
  val outputPathAT1 = "music_recommend/event/sns/affinity/at1/"
  val outputPathATN = "music_recommend/event/sns/affinity/atn/"
  val outputPathATI = "music_recommend/event/sns/affinity/ati/"
  val evtFEOutput = "music_recommend/event/frontend/output/"
  val evtFEBLACK = "music_recommend/event/frontend/black/"
  val evtFEOutputPatch = "music_recommend/event/frontend/output_patch/"
  val evtFEOutputBiz = "music_recommend/event/frontend/output_biz/"

  val batchClk = "music_recommend/moments/sns/features/clk_nearline"
  val affinityRankRollingUpdate = "music_recommend/event/sns/affinity/rank_rolling/"
  val affinityRankRollingUpdateTmp = "music_recommend/event/sns/affinity/rank_rolling_tmp/"

  val analyseOutput = "music_recommend/event/sns/evt/analyse/output/"
  val analyseOutputUv = "music_recommend/event/sns/evt/analyse/output_uv/"
  val analyseOutputDetail = "music_recommend/event/sns/evt/analyse/detail/output/"
  val analyseOutputUvDetail = "music_recommend/event/sns/evt/analyse/detail/output_uv/"


  val searchInfo = "music_recommend/moments/sns/features/search/ds="
  val searchInfoRead = "music_recommend/moments/sns/features/search"

  val labelPath = "music_recommend/moments/sns/label/"
  val predictOutput= "music_recommend/moments/sns/predict/"
  val modelOutput= "music_recommend/moments/sns/model/"


  val landingPageOutputArtist= "music_recommend/moments/sns/landingpage/artist/"
  val landingPageOutputNormal= "music_recommend/moments/sns/landingpage/normal/"
  val landingPageOutputNormalPre= "music_recommend/moments/sns/landingpage/normal_pre/"
  val landingPageOutputKOL= "music_recommend/moments/sns/landingpage/kol/"
  val landingPageOutputCF= "music_recommend/moments/sns/landingpage/cf/"
  val landingPageOutputPopStar= "music_recommend/moments/sns/landingpage/popStar/"
  val popularEventFeature= "music_recommend/event/sns/popular/event/feature/"
  val mutualPathBak = "music_recommend/event/sns/mutual/output_bak"
  val outputPathATBak = "music_recommend/event/sns/affinity/at_bak/"
  val outputPathATUBak = "music_recommend/event/sns/affinity/atu_bak/"


}
