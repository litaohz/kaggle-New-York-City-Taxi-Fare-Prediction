package com.netease.music.algo.event.model

import com.netease.music.algo.event.model.Path._
import org.apache.spark.SparkConf
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{LabeledPoint, MinMaxScaler, MinMaxScalerModel, VectorAssembler}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  */
object Evaluation {

  def inTestSplits = udf((logTime: Long) => {
    val testSeq = Seq(140, 141, 142, 143, 144, 145, 146, 147)
    testSeq.contains(logTime)
  })


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._
    val predictions = spark.read.parquet("/user/ndir/music_recommend/event/ml-re-sort/predict-result1")

    val predictionAndLabels = predictions.select("prediction", "label").map {
      row => (row.getDouble(0), row.getDouble(1))
    }.rdd
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    metrics.recallByThreshold()
    metrics.precisionByThreshold.take(1)
    metrics.fMeasureByThreshold(0.5).take(1)
    metrics.areaUnderPR
    val pipeline = PipelineModel.load(modelOutput)
    pipeline.stages(0).asInstanceOf[VectorAssembler].getInputCols
    pipeline.stages(1).asInstanceOf[MinMaxScalerModel].originalMax
    pipeline.stages(2).asInstanceOf[LogisticRegressionModel].coefficients
  }

}
