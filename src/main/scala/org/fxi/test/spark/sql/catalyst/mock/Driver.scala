package org.fxi.test.spark.sql.catalyst.mock


import org.apache.spark.sql.SparkSession
import org.fxi.test.spark.sql.datasets.Person

/**
  * Created by xifei on 16-10-14.
  */
object Driver {
  val spark = SparkSession
    .builder
    .appName("Spark SQL Example")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]) {
    val sparkMock: SparkSessionMock = new SparkSessionMock()
    LogUtil.doLog(" 准备Seq数据集合，case class类型,name 和 age 属性",this.getClass)
    val data = Seq(Person("Andy", 32), Person("jude", 20))
    LogUtil.doLog(" 创建DataSet",this.getClass)
    val mock: DataSetMock[Person] = sparkMock.createDataset[Person](data)
    LogUtil.doLog(" 对数据集DataSet操作,select,filter",this.getClass)
    val query =  mock.select($"age",$"name").filter($"age" > 20)

    println(query.queryExecution.optimizedPlan)
    println(query.queryExecution.executedPlan)

    query.collect()
  }
}

object LogUtil {
  var step: Int = 0

  def doLog(str: String,cls:Class[_]): Unit = {
    println(s"${step} ==> ${str}                             >>>>>>>" + cls)
    step = step + 1
  }
}
