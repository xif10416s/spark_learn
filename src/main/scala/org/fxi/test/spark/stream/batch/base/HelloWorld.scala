package org.fxi.test.spark.stream.batch.base

import com.alibaba.fastjson.JSON

/**
 * Created by xifei on 15-9-24.
 * java -classpath stream-1.0-SNAPSHOT.jar:/home/xifei/work/tools/scala/scala-2.10.5/lib/scala-library.jar com.moneylocker.data.analysis.online.stream.HelloWorld
 */
object HelloWorld {
  def main(args: Array[String]) {
    println("Hello, world!")
    val str2 = "{\"et\":\"kanqiu_client_join\",\"vtm\":1435898329434,\"body\":{\"client\":\"866963024862254\",\"client_type\":\"android\",\"room\":\"NBA_HOME\",\"gid\":\"\",\"type\":\"\",\"roomid\":\"\"},\"time\":1435898329}"
    val test =  JSON.parseObject(str2)
    println(test.getString("et"))
  }
}
