package org.fxi.test.deeplearn.bigdl.examples.lenet
import java.nio.file.Paths

import com.intel.analytics.bigdl.dataset.DataSet
import com.intel.analytics.bigdl.dataset.image.{BytesToGreyImg, GreyImgNormalizer, GreyImgToSample}
import com.intel.analytics.bigdl.nn.Module
import com.intel.analytics.bigdl.optim.Top1Accuracy
import com.intel.analytics.bigdl.utils.Engine
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.fxi.test.deeplearn.bigdl.examples.lenet.SparkIenetTrain.dataDir
object SparkIenetTest {
  import Utils._
  val dataDir= "D:\\deeplearning_dataset\\digits"
  def main(args: Array[String]): Unit = {
    testParser.parse(args, new TestParams()).foreach { param =>
      val conf = Engine.createSparkConf().setAppName("Test Lenet on MNIST")
        .set("spark.akka.frameSize", 64.toString)
        .set("spark.task.maxFailures", "1")
        .setMaster("local[*]")
      val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")
      Logger.getLogger("org").setLevel(Level.ERROR)
      Logger.getLogger("akka").setLevel(Level.ERROR)
      Logger.getLogger("breeze").setLevel(Level.ERROR)
      Engine.init

      val validationData = dataDir + "/t10k-images.idx3-ubyte"
      val validationLabel = dataDir + "/t10k-labels.idx1-ubyte"

      val partitionNum = 6
      val rddData = sc.parallelize(load(validationData, validationLabel), partitionNum)
      val transformer =
        BytesToGreyImg(28, 28) -> GreyImgNormalizer(testMean, testStd) -> GreyImgToSample()
      val evaluationSet = transformer(rddData)

      val model = Module.load[Float]("I:\\source\\learn\\spark\\tmp\\checkpoint\\bigdl\\20191208_101915\\model.10001")
      val result = model.evaluate(evaluationSet,
        Array(new Top1Accuracy[Float]), Some(param.batchSize))

      println(result.length +" ------------------------")
      result.foreach(r => println(s"${r._2} ---is--- ${r._1}"))
      sc.stop()
    }
  }
}
