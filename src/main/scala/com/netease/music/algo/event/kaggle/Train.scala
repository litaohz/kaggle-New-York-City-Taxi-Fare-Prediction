package com.netease.music.algo.event.kaggle

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.ml.feature.MinMaxScalerModel
object Train {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val fullDataset = spark.read.parquet("data/test/kaggle/basicinfo2")
    val assembler = new VectorAssembler().setInputCols(Array(
        "passenger_count",
        "trip_distance",
        "hour_passed",
        "week_month",
        "week_year",
        "day_week",
        "am_pm")).setOutputCol("assembledFeatures")
    val minMaxScaler = new MinMaxScaler().setInputCol(assembler.getOutputCol).setOutputCol("features")
    val trainingDataset = fullDataset.sample(withReplacement = false, 0.8)
    val testDataset = fullDataset.sample(withReplacement = false, 0.2)

    val lr = new LinearRegression().setMaxIter(200).setRegParam(0.01).setLabelCol("fare_amount")
    val pipeline = new Pipeline().setStages(Array(assembler, minMaxScaler, lr))

    val lrModel = pipeline.fit(trainingDataset)
    val predictions = lrModel.transform(testDataset)

    predictions.write.mode(SaveMode.Overwrite).parquet("data/test/kaggle/predict0")

    // Select example rows to display.
    //    predictions.select("prediction", "label", "features").show(5)

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label")

    val f1 = evaluator.evaluate(predictions)
    println("f1 = " + f1)

    evaluator.setMetricName("accuracy")

    val precision = evaluator.evaluate(predictions)

    println("precision = " + precision)

    lrModel.write.overwrite().save("data/test/kaggle/model0")
    lrModel.stages(2).asInstanceOf[LinearRegressionModel].coefficients
    lrModel.stages(1).asInstanceOf[MinMaxScalerModel].getInputCol
    lrModel.stages(0).asInstanceOf[VectorAssembler].getInputCols
  }

  /*

   */


}
