package org.fxi.test.spark.ml.mmlspark.classification

import com.microsoft.ml.spark.{ComputeModelStatistics, TrainedClassifierModel, TrainClassifier}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.{Row, Dataset, DataFrame, SparkSession}

/**
  * @author seki
  * @date 18/11/16
  */
object LRPreditIncomes {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Spark Pi")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error")


    val csvDf: DataFrame = spark.read.option("header", "true").csv("scalaProject/data/AdultCensusIncome.csv").select(" education", " marital-status", " hours-per-week", " income").cache()
    csvDf.show()
    val Array(train,test) = csvDf.randomSplit(Array(0.75,0.25),123L)
    val model: TrainedClassifierModel = new TrainClassifier()
      .setModel(new LogisticRegression())
      .setLabelCol(" income").setNumFeatures(256).fit(train)



    println(model.featuresColumn)
    println(model.labelColumn)
    model.levels.get.foreach(println _)
    val prediction: DataFrame = model.transform(test)
    prediction.show()
    println(prediction.first())
     val transform: DataFrame = new ComputeModelStatistics().transform(prediction)
    transform.show()
  }
}
