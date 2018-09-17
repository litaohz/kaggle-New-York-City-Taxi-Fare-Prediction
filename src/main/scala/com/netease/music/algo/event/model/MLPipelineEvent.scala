package com.netease.music.algo.event.model

import java.util.Calendar

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.sql.{SaveMode, SparkSession}
import Path._
import org.apache.spark.sql.functions.udf

/**
  */
object MLPipelineEvent {

  def inTestSplits = udf((logTime: Long) => {
    val testSeq = Seq(152, 151, 150, 149, 148, 147)
    testSeq.contains(logTime)
  })


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._




    // "score", "zanCount", "commentCount", "forwardCount", "smoothClickRate",
    // "zanRate", "commentRate", "forwardRate",
    // "eventLanguageVector", "eventCategoryVector"
    // "crossLanguageInEventVector", "crossCategoryVector"
    //    "zanRate", "commentRate", "forwardRate",
    //    "isSubArtist", "isRecArtist"
    val assembler = new VectorAssembler()
      .setInputCols(Array("zanCount", "commentCount", "forwardCount","zanRate","commentRate","forwardRate",
        "smoothClickRate", "logScore", "isSubArtist", "isRecArtist", "clickRateForUserAge", "clickRateForUserGender"))
      .setOutputCol("assembledFeatures")

    val minMaxScaler = new MinMaxScaler()
      .setInputCol(assembler.getOutputCol)
      .setOutputCol("features")

    val fullDataset = spark.read.parquet(dataSetOutput)
    val trainingDataset = fullDataset.filter(!inTestSplits($"logDay"))

    val testDataset = fullDataset.filter(inTestSplits($"logDay"))

    // 逻辑回归
    val lr = new LogisticRegression()
      .setMaxIter(200)
      .setRegParam(0.01)

    // 随机森林
    val rf = new RandomForestClassifier().setNumTrees(10)

    val pipeline = new Pipeline().setStages(Array(assembler, minMaxScaler, lr))

    val lrModel = pipeline.fit(trainingDataset)

    val predictions = lrModel.transform(testDataset)

    predictions.write.mode(SaveMode.Overwrite).parquet(predictOutPut)

    // Select example rows to display.
    //    predictions.select("prediction", "label", "features").show(5)

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label")

    val f1 = evaluator.evaluate(predictions)
    println("f1 = " + f1)

    evaluator.setMetricName("accuracy")

    val precision = evaluator.evaluate(predictions)

    println("precision = " + precision)

    lrModel.write.overwrite().save(modelOutput)

  }

}
