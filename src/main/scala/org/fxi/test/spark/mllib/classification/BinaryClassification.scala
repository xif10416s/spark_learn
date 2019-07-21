package org.fxi.test.spark.mllib.classification

import org.fxi.test.spark.mllib.AbstractParams
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{SVMWithSGD, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.optimization.{SquaredL2Updater, L1Updater}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

/**
  * Created by xifei on 16-5-6.
  * 分类
  * 输出值是
  */
object BinaryClassification {

  object Algorithm extends Enumeration {
    type Algorithm = Value
    val SVM, LR = Value
  }

  object RegType extends Enumeration {
    type RegType = Value
    val L1, L2 = Value
  }


  import Algorithm._
  import RegType._

  case class Params(
                     input: String = "./spark/data/mllib/sample_binary_classification_data.txt",
                     numIterations: Int = 100,
                     stepSize: Double = 1.0,
                     algorithm: Algorithm = LR,
                     regType: RegType = L2,
                     regParam: Double = 0.01) extends AbstractParams[Params]


  def main(args: Array[String]): Unit = {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("BinaryClassification") {
      head("BinaryClassification: an example app for binary classification.")
      opt[Int]("numIterations")
        .text("number of iterations")
        .action((x, c) => c.copy(numIterations = x))
      opt[Double]("stepSize")
        .text("initial step size (ignored by logistic regression), " +
          s"default: ${defaultParams.stepSize}")
        .action((x, c) => c.copy(stepSize = x))
      opt[String]("algorithm")
        .text(s"algorithm (${Algorithm.values.mkString(",")}), " +
          s"default: ${defaultParams.algorithm}")
        .action((x, c) => c.copy(algorithm = Algorithm.withName(x)))
      opt[String]("regType")
        .text(s"regularization type (${RegType.values.mkString(",")}), " +
          s"default: ${defaultParams.regType}")
        .action((x, c) => c.copy(regType = RegType.withName(x)))
      opt[Double]("regParam")
        .text(s"regularization parameter, default: ${defaultParams.regParam}")

      note(
        """
          |For example, the following command runs this app on a synthetic dataset:
          |
          | bin/spark-submit --class org.apache.spark.examples.mllib.BinaryClassification \
          |  examples/target/scala-*/spark-examples-*.jar \
          |  --algorithm LR --regType L2 --regParam 1.0 \
          |  data/mllib/sample_binary_classification_data.txt
        """.stripMargin)
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      sys.exit(1)
    }

    def run(params: Params) {
      val conf = new SparkConf().setAppName(s"BinaryClassification with $params").setMaster("local[*]")
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

      val model = params.algorithm match {
        case LR =>
          val algorithm = new LogisticRegressionWithLBFGS()
          algorithm.optimizer
            .setNumIterations(params.numIterations)
            .setUpdater(updater)
            .setRegParam(params.regParam)
          algorithm.run(training).clearThreshold()
        case SVM =>
          val algorithm = new SVMWithSGD()//
          algorithm.optimizer
            .setNumIterations(params.numIterations)
            .setStepSize(params.stepSize)
            .setUpdater(updater)
            .setRegParam(params.regParam)
          algorithm.run(training).clearThreshold()
      }

      val prediction = model.predict(test.map(_.features))
      val predictionAndLabel = prediction.zip(test.map(_.label))
      predictionAndLabel.foreach(println _)

      val metrics = new BinaryClassificationMetrics(predictionAndLabel)



      // Precision by threshold
      val precision = metrics.precisionByThreshold
      precision.foreach { case (t, p) =>
        println(s"Threshold: $t, Precision: $p")
      }

      // Recall by threshold
      val recall = metrics.recallByThreshold
      recall.foreach { case (t, r) =>
        println(s"Threshold: $t, Recall: $r")
      }

      // Precision-Recall Curve
      val PRC = metrics.pr

      // F-measure
      val f1Score = metrics.fMeasureByThreshold
      f1Score.foreach { case (t, f) =>
        println(s"Threshold: $t, F-score: $f, Beta = 1")
      }

      val beta = 0.5
      val fScore = metrics.fMeasureByThreshold(beta)
      f1Score.foreach { case (t, f) =>
        println(s"Threshold: $t, F-score: $f, Beta = 0.5")
      }

      // AUPRC
      val auPRC = metrics.areaUnderPR
      println("Area under precision-recall curve = " + auPRC)

      // Compute thresholds used in ROC and PR curves
      val thresholds = precision.map(_._1)

      // ROC Curve
      val roc = metrics.roc

      // AUROC
      val auROC = metrics.areaUnderROC
      println("Area under ROC = " + auROC)

      sc.stop()
    }

  }

}
