package com.netease.music.algo.event.tmp

import com.netease.music.algo.event.path.OutputPath._
import com.netease.music.algo.event.schema.BaseSchema._
import com.netease.music.algo.event.udf.Udf1._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.netease.music.algo.event.model.Udf.testUserid
import org.apache.spark.sql.functions.{concat_ws, udf}

object Mock1 {
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val res0 = spark.read.option("sep", "\t").schema(commonSchema).csv("music_recommend/event/sns/output1/".concat(args(0)))
    res0.where(!testUserid($"userid")).withColumn("result",changeAlg($"result")).
      select(concat_ws("\t", $"userId", $"result")).
      write.mode(SaveMode.Overwrite).text("music_recommend/event/sns/outputtest/")

  }
  def testUserid = udf((userid: Long) => {
    userid % 10 == 3
  })
  def changeAlg = udf((result: String) => {
    val resArray = result.split("-")
    var newResArray = resArray(0).split(",").map(x=>{
      var res = x.split(":")
      if(res(1).equals("o")){
        res(1) = "or"
      }
      res.mkString(":")
    })
    newResArray.mkString(",").concat("-").concat(resArray(1)).concat("-").concat(resArray(2))
  })

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf).enableHiveSupport()
    .getOrCreate()
}
