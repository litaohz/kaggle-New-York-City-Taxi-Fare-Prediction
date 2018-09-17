package com.netease.music.algo.event.backup

import com.netease.music.algo.event.path.OutputPath.{labelPath, modelOutput}
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

object Misc {

  val conf = new SparkConf()
  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()
  def main(args: Array[String]): Unit = {
    VectorAssembler
    LogisticRegressionModel
    val pipeline = PipelineModel.load(modelOutput)

    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("recall")

    evaluator.evaluate(pipeline.transform(spark.read.parquet(labelPath)))
  }
}
