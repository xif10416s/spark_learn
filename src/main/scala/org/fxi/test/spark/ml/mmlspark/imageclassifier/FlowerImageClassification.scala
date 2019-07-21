package org.fxi.test.spark.ml.mmlspark.imageclassifier

import com.microsoft.ml.spark.ImageTransformer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @author seki
  * @date 18/11/15
  */
object FlowerImageClassification {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Spark Pi")
      .master("local[*]")
      .getOrCreate()

    val imagesWithLabels = spark
      .read.parquet("wasbs://publicwasb@mmlspark.blob.core.windows.net/flowers_and_labels_small.parquet")
      .withColumn("labels", col("labels").cast("Double"))

    ImageTransformer
    println("a")
  }
}
