package com.netease.music.algo.event.top.features

import org.apache.spark.sql.functions.udf

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse


object Udf {

  def getArtist = udf((artistInfo: String) => {
    artistInfo.split(":")(0)
  })

  /*
  {"正趣果上果":{"song":{"30431370":{"id":30431370,"os":"android","logtime":1524579276,
  "anttypes":{"total":2,"click":1,"ps_count":1,"skw":1},"skw_time":1524579276,"pos":0}}}}
   */
  def getSongId = udf((songInfo: String) => {
    val idx = songInfo.indexOf("song")
    val str2 = songInfo.substring(idx + 8)
    str2.substring(0, str2.indexOf("\""))
  })

  def main(args: Array[String]): Unit = {
    val songInfo = "{\"正趣果上果\":{\"video\":{\"30431370\":{\"id\":30431370,\"os\":\"android\",\"logtime\":1524579276,\"anttypes\":{\"total\":2,\"click\":1,\"ps_count\":1,\"skw\":1},\"skw_time\":1524579276,\"pos\":0}}}}\t"
    val idx = songInfo.indexOf("song")
    val str2 = songInfo.substring(idx + 8)
    str2.substring(0, str2.indexOf("\""))
    println(str2.substring(0, str2.indexOf("\"")).isEmpty)
  }


}
