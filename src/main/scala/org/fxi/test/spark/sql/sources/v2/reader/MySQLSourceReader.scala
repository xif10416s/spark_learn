package org.fxi.test.spark.sql.sources.v2.reader
import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD}
import org.apache.spark.sql.sources.{EqualTo, Filter, GreaterThan, IsNotNull}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.JavaConverters._

case class MySQLSourceReader(options: Map[String, String])
  extends DataSourceReader with SupportsPushDownRequiredColumns with SupportsPushDownFilters {

  /**
    * 支持算子下推的过滤器，扫描之前过滤，减少数据量
    */
  val supportedFilters: ArrayBuffer[Filter] = ArrayBuffer[Filter]()

  var requiredSchema: StructType = {
    val jdbcOptions = new JDBCOptions(options)
    JDBCRDD.resolveTable(jdbcOptions)
  }

  override def readSchema(): StructType = {
    requiredSchema
  }

  /**
    * 返回InputPartition 列表，每一个InputPartition负责创建一个data reader，用来创建一个输出数据的RDD
    * InputPartition的数量与产生的RDD分区数量一样
    * 算子下推，列剪裁等一些优化操作可以在InputPartition中实现，不一定是全表扫描
    *
    */
  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = List[InputPartition[InternalRow]](getPartitionList():_*).asJava

  /**
    * 自定义分区实现，按照id分割
    * @return
    */
  def getPartitionList():List[InputPartition[InternalRow]]= {
    var start: Int = options("start").toInt
    val end: Int = options("end").toInt
    val split: String = options("split")
    val list = new ListBuffer[InputPartition[InternalRow]]

    val total = end - start
    val delta = total/split.toInt
    while(start < end){
      var to = start + delta
      if(start + delta > end){
        to = end
      }
      list+=MySQLInputPartition(requiredSchema, supportedFilters.toArray,start ,to , options)
      start=start + delta
    }

    list.toList
  }
  /**
    * Pushes down filters, and returns filters that need to be evaluated after scanning.
    * 扫描之后需要使用的过滤器， 也就是不支持算子下推的过滤器
    * @param filters
    * @return
    */
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    if (filters.isEmpty) {
      return filters
    }

    val unsupportedFilters = ArrayBuffer[Filter]()

    filters.foreach {
      case f: EqualTo => supportedFilters += f
      case f: GreaterThan => supportedFilters += f
      case f: IsNotNull => supportedFilters += f
      case f@_ => unsupportedFilters += f
    }

    unsupportedFilters.toArray
  }

  /**
    * 支持算子下推的过滤器，扫描之前过滤，减少数据量
    * * There are 3 kinds of filters:
    * *  1. pushable filters which don't need to be evaluated again after scanning.
    * *  2. pushable filters which still need to be evaluated after scanning, e.g. parquet
    * *     row group filter.
    * *  3. non-pushable filters.
    * * Both case 1 and 2 should be considered as pushed filters and should be returned by this method.
    */
  override def pushedFilters(): Array[Filter] = supportedFilters.toArray

  /**
    * 裁剪不需要的列
    * @param requiredSchema
    */
  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }
}
