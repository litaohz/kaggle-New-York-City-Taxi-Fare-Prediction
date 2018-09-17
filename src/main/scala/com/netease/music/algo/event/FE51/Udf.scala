package com.netease.music.algo.event.FE51

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

object Udf {


  def getResIdFromContent: UserDefinedFunction = udf((content: String) => {
    implicit val formats = DefaultFormats
    val contentJson = (parse(content))
    (contentJson \ "resourceId").extractOrElse[Long](0)
  })


  def getResTypeFromContent: UserDefinedFunction = udf((content: String) => {
    implicit val formats = DefaultFormats
    val contentJson = (parse(content))
    (contentJson \ "resourceType").extractOrElse[Long](0)
  })


  def getSongId: UserDefinedFunction = udf((songIds: String) => {
    songIds.drop(1).dropRight(1).split(",")
  })

  def concat_ws1: UserDefinedFunction = udf((id: Long, blockType: Long) => {
    String.valueOf(id).concat(":").concat("1.0").concat(":").concat(String.valueOf(blockType))
  })


  def main(args: Array[String]): Unit = {
    println("eventId", "artistId", "creatorid", "resourceId", "songId")
  }


  def toString1: UserDefinedFunction = udf((value: Long) => {
    String.valueOf(value)
  })
}
