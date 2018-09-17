package com.netease.music.algo.event.model

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.netease.music.algo.event.path.OutputPath._
import Udf._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

object V1 {

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val dataset = spark.read.parquet(labelPath).withColumn("isTest",testUserid($"userid"))
    val trainingDataset = dataset.where("isTest = false")
    val testDataset = dataset.where("isTest = true")
    val assembler = new VectorAssembler().
      setInputCols(Array("time_dist","from_phone","from_artist","from_star","from_search","from_song","from_weibo","evt_size","evt_dist","cnt","cnt4","cnt7","cnt15","cnt31")).
      setOutputCol("features")

    val lr = new LogisticRegression().setMaxIter(200).setRegParam(0.01).setLabelCol("label").setFeaturesCol("features")

    val pipeline = new Pipeline().setStages(Array(assembler, lr))


    val lrModel = pipeline.fit(trainingDataset)

    val predictions = lrModel.transform(testDataset)

    predictions.write.mode(SaveMode.Overwrite).parquet(predictOutput)

    // Select example rows to display.
    //    predictions.select("prediction", "label", "features").show(5)

    // Select (prediction, true label) and compute test error.
    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label")

    val auc = evaluator.evaluate(predictions)
    println("areaUnderROC = " + auc)

    lrModel.write.overwrite().save(modelOutput)

  }

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf).enableHiveSupport()
    .getOrCreate()

}
