package org.fxi.test.deeplearn.bigdl.examples.lenet

import com.intel.analytics.bigdl._
import com.intel.analytics.bigdl.dataset.DataSet
import com.intel.analytics.bigdl.dataset.image.{BytesToGreyImg, GreyImgNormalizer, GreyImgToBatch}
import com.intel.analytics.bigdl.models.lenet.LeNet5
import com.intel.analytics.bigdl.nn.{ClassNLLCriterion, CrossEntropyCriterion, Module}
import com.intel.analytics.bigdl.numeric.NumericFloat
import com.intel.analytics.bigdl.optim._
import com.intel.analytics.bigdl.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.fxi.test.deeplearn.bigdl.examples.lenet.Utils._

object SparkIenetTrain {
  val dataDir= "D:\\deeplearning_dataset\\digits"
  val trainData =dataDir  + "/train-images.idx3-ubyte"
  val trainLabel = dataDir + "/train-labels.idx1-ubyte"
  val validationData = dataDir + "/t10k-images.idx3-ubyte"
  val validationLabel = dataDir + "/t10k-labels.idx1-ubyte"


  def main(args: Array[String]): Unit = {
    trainParser.parse(args, new TrainParams()).map(param => {
      val conf = Engine.createSparkConf()
        .setAppName("Train Lenet on MNIST")
        .set("spark.task.maxFailures", "1")
        .setMaster("local[*]")
      val sc = new SparkContext(conf)
      Engine.init

//      val trainData = param.folder + "/train-images-idx3-ubyte"
//      val trainLabel = param.folder + "/train-labels-idx1-ubyte"
//      val validationData = param.folder + "/t10k-images-idx3-ubyte"
//      val validationLabel = param.folder + "/t10k-labels-idx1-ubyte"

      val model = if (param.modelSnapshot.isDefined) {
        Module.load[Float](param.modelSnapshot.get)
      } else {
        LeNet5(classNum = 10)
      }
      val criterion =  ClassNLLCriterion()


      val optimMethod = if (param.stateSnapshot.isDefined) {
        OptimMethod.load[Float](param.stateSnapshot.get)
      } else {
        new SGD[Float](learningRate = param.learningRate,
          learningRateDecay = param.learningRateDecay)
      }

      val trainSet = DataSet.array(load(trainData, trainLabel), sc) ->
        BytesToGreyImg(28, 28) -> GreyImgNormalizer(trainMean, trainStd) -> GreyImgToBatch(
        param.batchSize)

      val optimizer = Optimizer(
        model = model,
        dataset = trainSet,
        criterion = criterion )
//      if (param.checkpoint.isDefined) {
//        optimizer(param.checkpoint.get, Trigger.everyEpoch)
//      }
//      if(param.overWriteCheckpoint) {
//        optimizer.overWriteCheckpoint()
//      }
      optimizer.setCheckpoint("I:\\source\\learn\\spark\\tmp\\checkpoint\\bigdl", Trigger.everyEpoch)
      optimizer.overWriteCheckpoint()
      val validationSet = DataSet.array(load(validationData, validationLabel), sc) ->
        BytesToGreyImg(28, 28) -> GreyImgNormalizer(testMean, testStd) -> GreyImgToBatch(
        param.batchSize)

      optimizer
        .setValidation(
          trigger = Trigger.everyEpoch,
          dataset = validationSet,
          vMethods = Array(new Top1Accuracy, new Top5Accuracy[Float], new Loss[Float]))
        .setOptimMethod(optimMethod)
        .setEndWhen(Trigger.maxEpoch(param.maxEpoch))
        .optimize()

      sc.stop()
    })
  }
}
