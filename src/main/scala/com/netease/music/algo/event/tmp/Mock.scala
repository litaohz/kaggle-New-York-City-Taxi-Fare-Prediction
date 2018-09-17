package com.netease.music.algo.event.tmp

import com.netease.music.algo.event.path.OutputPath._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.netease.music.algo.event.schema.BaseSchema._
import com.netease.music.algo.event.udf.Udf1._
import org.apache.spark.sql.functions.concat_ws

object Mock {
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val str = "3485139463:9:1050425:10024228,3497046986:9:893259:1344105258,3454968822:9:893259:90562321,2435081034:9:6462:95161139,2354278920:9:5001:330434258,3502052326:9:12760978:304620685,2443818799:9:3685:127836080,3485229207:9:1137098:513920143,2399752066:9:12146142:320123275,3471921307:9:2159:318470247,3502293220:9:5774:539923903,3460794880:9:2159:492688094,2266337187:9:5001:95076,3449024752:9:6066:621063978,2428290925:9:784453:101262078,3480670113:9:6066:344996275,3495407248:9:13112043:535402244,2438224997:9:784453:250988491,3502757371:9:12932368:396277923,2439240231:9:12146142:40695951,3524862974:9:840134:250988491"
    val userid = spark.read.option("sep", "|").schema(commonSchema1).csv("/user/ndir/litao03/music_userId_u_10w.txt")
    userid.withColumn("eventInfo", concatUserid(str)($"userid")).drop("userid").
      write.mode(SaveMode.Overwrite).text(evtFEOutputPatch)

  }

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf).enableHiveSupport()
    .getOrCreate()
}
