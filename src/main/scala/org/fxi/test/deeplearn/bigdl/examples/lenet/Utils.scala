package org.fxi.test.deeplearn.bigdl.examples.lenet

import java.nio.ByteBuffer
import java.nio.file.{Files, Path, Paths}

import com.intel.analytics.bigdl.dataset.ByteRecord
import com.intel.analytics.bigdl.utils.File
import scopt.OptionParser

object Utils {
  val trainMean = 0.13066047740239506
  val trainStd = 0.3081078

  val testMean = 0.13251460696903547
  val testStd = 0.31048024

  case class TrainParams(
                          folder: String = "./",
                          checkpoint: Option[String] = None,
                          modelSnapshot: Option[String] = None,
                          stateSnapshot: Option[String] = None,
                          batchSize: Int = 12,
                          learningRate: Double = 0.05,
                          learningRateDecay: Double = 0.0,
                          maxEpoch: Int = 2,
                          coreNumber: Int = -1,
                          nodeNumber: Int = -1,
                          overWriteCheckpoint: Boolean = false,
                          graphModel: Boolean = false
                        )

  val trainParser = new OptionParser[TrainParams]("BigDL Lenet Train Example") {
    opt[String]('f', "folder")
      .text("where you put the MNIST data")
      .action((x, c) => c.copy(folder = x))
    opt[Int]('b', "batchSize")
      .text("batch size")
      .action((x, c) => c.copy(batchSize = x))
    opt[String]("model")
      .text("model snapshot location")
      .action((x, c) => c.copy(modelSnapshot = Some(x)))
    opt[String]("state")
      .text("state snapshot location")
      .action((x, c) => c.copy(stateSnapshot = Some(x)))
    opt[String]("checkpoint")
      .text("where to cache the model")
      .action((x, c) => c.copy(checkpoint = Some(x)))
    opt[Double]('r', "learningRate")
      .text("learning rate")
      .action((x, c) => c.copy(learningRate = x))
    opt[Double]('d', "learningRateDecay")
      .text("learning rate decay")
      .action((x, c) => c.copy(learningRateDecay = x))
    opt[Int]('e', "maxEpoch")
      .text("epoch numbers")
      .action((x, c) => c.copy(maxEpoch = x))
    opt[Int]('b', "batchSize")
      .text("batch size")
      .action((x, c) => c.copy(batchSize = x))
    opt[Unit]("overWrite")
      .text("overwrite checkpoint files")
      .action( (_, c) => c.copy(overWriteCheckpoint = true) )
    opt[Unit]('g', "graphModel")
      .text("use graph model")
      .action((x, c) => c.copy(graphModel = true))
  }

  case class TestParams(
                         folder: String = "./",
                         model: String = "",
                         batchSize: Int = 6
                       )

  val testParser = new OptionParser[TestParams]("BigDL Lenet Test Example") {
//    opt[String]('f', "folder")
//      .text("where you put the MNIST data")
//      .action((x, c) => c.copy(folder = x))
//
//    opt[String]("model")
//      .text("model snapshot location")
//      .action((x, c) => c.copy(model = x))
//      .required()
//      .required()
    opt[Int]('b', "batchSize")
      .text("batch size")
      .action((x, c) => c.copy(batchSize = x))
  }

  /**
    * load mnist data.
    * read mnist from hdfs if data folder starts with "hdfs:", otherwise form local file.
    * @param featureFile
    * @param labelFile
    * @return
    */
  val hdfsPrefix: String = "hdfs:"
  private[bigdl] def load(featureFile: String, labelFile: String): Array[ByteRecord] = {

    val featureBuffer = if (featureFile.startsWith(hdfsPrefix)) {
      ByteBuffer.wrap(File.readHdfsByte(featureFile))
    } else {
      ByteBuffer.wrap(Files.readAllBytes(Paths.get(featureFile)))
    }
    val labelBuffer = if (featureFile.startsWith(hdfsPrefix)) {
      ByteBuffer.wrap(File.readHdfsByte(labelFile))
    } else {
      ByteBuffer.wrap(Files.readAllBytes(Paths.get(labelFile)))
    }
    val labelMagicNumber = labelBuffer.getInt()

    require(labelMagicNumber == 2049)
    val featureMagicNumber = featureBuffer.getInt()
    require(featureMagicNumber == 2051)

    val labelCount = labelBuffer.getInt()
    val featureCount = featureBuffer.getInt()
    require(labelCount == featureCount)

    val rowNum = featureBuffer.getInt()
    val colNum = featureBuffer.getInt()

    val result = new Array[ByteRecord](featureCount)
    var i = 0
    while (i < featureCount) {
      val img = new Array[Byte]((rowNum * colNum))
      var y = 0
      while (y < rowNum) {
        var x = 0
        while (x < colNum) {
          img(x + y * colNum) = featureBuffer.get()
          x += 1
        }
        y += 1
      }
      result(i) = ByteRecord(img, labelBuffer.get().toFloat + 1.0f)
      i += 1
    }

    result
  }
}
