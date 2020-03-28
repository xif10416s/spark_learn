/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.fxi.test.spark.stream.batch.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

/**
  * 启动本地kafka:
  *  zookeeper启动 ： bin\windows\zookeeper-server-start.bat config\zookeeper.properties
  *  kafka启动： bin\windows\kafka-server-start.bat config\server.properties
  *  创建topic:  bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4 --topic test
  *  发送消息： bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic test
  *  查看offset : bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group test_group
 *
  *  ENABLE_AUTO_COMMIT_CONFIG 为false 的情况，并且consumer_group之前没有提交offset,AUTO_OFFSET_RESET_CONFIG 设置会生效：earliest 最早，latest 最新
  * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: DirectKafkaWordCount <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <groupId> is a consumer group name to consume from topics
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
 *    consumer-group topic1,topic2
  *    localhost:9092 test_group test
 */
object DirectKafkaExactOnceWordCount {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println(s"""
        |Usage: DirectKafkaWordCount <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <groupId> is a consumer group name to consume from topics
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
      System.exit(1)
    }


    val Array(brokers, groupId, topics) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.sparkContext.setLogLevel("ERROR")


    // 1 从数据库读取partition对应的offset
    val selectOffsetsFromYourDatabase = List(("test",0,6),("test",1,9),("test",2,3),("test",3,3))
    // begin from the the offsets committed to the database
    // 2  转换成 TopicPartition
    val fromOffsets = selectOffsetsFromYourDatabase.map { resultSet =>
      new TopicPartition(resultSet._1, resultSet._2) -> resultSet._3.toLong
    }.toMap

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      //在任何情况下把这两个值设置为earliest或者latest ,消费者就可以从最早或者最新的offset开始消费,但在实际上测试的时候发现并不是那么回事,因为他们生效都有一个前提条件,那就是对于同一个groupid的消费者,如果这个topic某个分区有已经提交的offset
      // ,那么无论是把auto.offset.reset=earliest还是latest,都将失效,消费者会从已经提交的offset开始消费
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest" , // "earliest": 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费    , "latest" : 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false" , // 是否自动commit offset,默认true
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])
    // 3 初始化stream
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
//      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
        )
    // 4 逻辑处理
    // 一个rdd 一个批次
    messages.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      // 4.1 rdd 计算逻辑
      val resultRdd = rdd.map(_.value()).flatMap(_.split(" "))
        .map(x => (x, 1L)).reduceByKey(_ + _)
      // 4.2 开启事务
      // conn.beginTransaction()

      // 一，全量处理方式,结果集不大，收集到driver保存，所有partition一起处理
      // 处理结果保存
      // save to db
//      resultRdd.collect().foreach(println _)
//
//      //  offset 更新，保存
//      offsetRanges.foreach(o =>{
//        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
//      })

      // 二，按分区处理，结果集大的时候，executor保存，单个partition处理
      resultRdd.foreachPartition(f =>{
        f.foreach(println(_))
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      })




      // 4.5 提交事务
      // conn.commit()

    })
    val lines = messages.map(_.value)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println
