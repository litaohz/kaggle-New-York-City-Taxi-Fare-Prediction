package com.netease.music.algo.event.v1.link.predict.model

import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.netease.music.algo.event.v1.Path._
object TrainLR {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val assembler = new VectorAssembler()
      .setInputCols(Array("dislike", "impress", "click_photo","from_song","from_local"))
      .setOutputCol("features")


  val trainingDataset = spark.read.parquet(trainingData)

    // 逻辑回归
    val lr = new LogisticRegression()
      .setMaxIter(200)
      .setRegParam(0.01)

    // 随机森林
//    val rf = new RandomForestClassifier().setNumTrees(10)

    val pipeline = new Pipeline().setStages(Array(assembler, lr))

    val lrModel = pipeline.fit(trainingDataset)
//
//    val predictions = lrModel.transform(testDataset)
//
//    predictions.write.mode(SaveMode.Overwrite).parquet(predictOutPut)
//
//    // Select example rows to display.
//    //    predictions.select("prediction", "label", "features").show(5)
//
//    // Select (prediction, true label) and compute test error.
//    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label")
//
//    val f1 = evaluator.evaluate(predictions)
//    println("f1 = " + f1)
//
//    evaluator.setMetricName("accuracy")
//
//    val precision = evaluator.evaluate(predictions)
//
//    println("precision = " + precision)

    lrModel.write.overwrite().save(modelOutput.concat(args(0)))

  }

}
