package org.fix.test.jyyb

import org.apache.spark.sql.SparkSession
import org.fix.test.jyyb.BaseStat.spark

import scala.collection.mutable.ListBuffer

object TestTuanhuo {
  val spark = SparkSession
    .builder
    .appName("Spark Pi")
    .master("local[*]")
    .config("driver-memory","12g")
    .config("executor-memory","12g")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    //C:\Users\DELL\Desktop
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    val df = spark.read.format("csv").option("header", "true").option("charset", "gbk").load("C:\\Users\\DELL\\Desktop/jianyi2.csv")
    val idMap = scala.collection.mutable.Map[String,java.util.BitSet]()

    val df2 = spark.read.format("csv").option("charset", "gbk").load("C:\\Users\\DELL\\Desktop/sss.csv")


    val warnCount = 4
    df.collect().foreach(f =>{
      val bitSet = new java.util.BitSet(161)
      val id = f.getString(0)
      for(i <- 1 to 161){
        if(f.getString(i).toInt == 1){
          bitSet.set(i)
        }
      }
      idMap.put(id,bitSet)
    })
    println(idMap.get("8100074915").get)

    val targetMap = idMap.filter(p => p._2.cardinality()>=10 )

    val bdIdMap = spark.sparkContext.broadcast(idMap)

    for(k <- 5 to 13) {
      val ds = df2.map(f => {
        val ls = ListBuffer[String]()
        for (i <- 0 until k) {
          ls += f.getString(i)
        }
        ls
      })

      //    val iterator = targetMap.keySet.toList.combinations(10).slice(0,100).toList
      //    val ds = iterator.toDS().repartition(200)

      val rsDs = ds.map(f =>{
        val firstUser = f(0)
        val firstUserBitSet = bdIdMap.value.get(firstUser).get
        val cad = firstUserBitSet.cardinality()
        var maxTime = cad
        var minTime = cad
        f.slice(1,f.size).foreach(user =>{
          val userBitMap = bdIdMap.value.get(user).get
          val userCard = userBitMap.cardinality()
          maxTime = Math.max(maxTime,userCard)
          minTime = Math.min(minTime,userCard)
          firstUserBitSet.and(bdIdMap.value.get(user).get)
        })
        val mergeCand = firstUserBitSet.cardinality()
        if(mergeCand >= warnCount){
          val rs = s"${String.join(",",f:_*)} ,  ${mergeCand} ,  ${maxTime} , ${minTime}"
          println(rs)
          rs
        } else {
          ""
        }
      }).filter(_ != "")

      rsDs.collect().foreach(println _)
    }


  }
}
