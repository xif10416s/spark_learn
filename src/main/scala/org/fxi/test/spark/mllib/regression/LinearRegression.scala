package org.fxi.test.spark.mllib.regression

import org.fxi.test.spark.mllib.AbstractParams
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.optimization.{L1Updater, SquaredL2Updater}
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by xifei on 16-5-6.
  */
object LinearRegression {
  import org.fxi.test.spark.mllib.classification.BinaryClassification.RegType._

  case class Params(
                     input: String = "./spark/data/mllib/sample_linear_regression_data.txt",
                     numIterations: Int = 100,
                     stepSize: Double = 1.0,
                     regType: RegType = L2,
                     regParam: Double = 0.01) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()
    run(defaultParams)
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"LinearRegression with $params").setMaster("local[*]")
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    val examples = MLUtils.loadLibSVMFile(sc, params.input).cache()

    val splits = examples.randomSplit(Array(0.8, 0.2))
    val training = splits(0).cache()
    val test = splits(1).cache()

    val numTraining = training.count()
    val numTest = test.count()
    println(s"Training: $numTraining, test: $numTest.")

    examples.unpersist(blocking = false)

    val updater = params.regType match {
      case L1 => new L1Updater()
      case L2 => new SquaredL2Updater()
    }

    val algorithm = new LinearRegressionWithSGD()
    algorithm.optimizer
      .setNumIterations(params.numIterations)
      .setStepSize(params.stepSize)
      .setUpdater(updater)
      .setRegParam(params.regParam)

    val model = algorithm.run(training)

    val prediction = model.predict(test.map(_.features))
    val predictionAndLabel = prediction.zip(test.map(_.label))

    val loss = predictionAndLabel.map { case (p, l) =>
      val err = p - l
      println(p +"  " + l)
      err * err
    }.reduce(_ + _)
    val rmse = math.sqrt(loss / numTest)

    println(s"Test RMSE = $rmse.")

    //---------
    // Instantiate metrics object
    val metrics = new RegressionMetrics(predictionAndLabel)

    // Squared error
    println(s"MSE = ${metrics.meanSquaredError}")
    println(s"RMSE = ${metrics.rootMeanSquaredError}")

    // R-squared
    println(s"R-squared = ${metrics.r2}")

    // Mean absolute error
    println(s"MAE = ${metrics.meanAbsoluteError}")

    // Explained variance
    println(s"Explained variance = ${metrics.explainedVariance}")

    sc.stop()
  }
}
