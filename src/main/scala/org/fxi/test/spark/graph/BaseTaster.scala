package org.fxi.test.spark.graph

import org.apache.spark.SparkContext
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

import scala.math._

/**
  * Created by seki on 16/11/8.
  */
class BaseTaster extends FunSuite {

  test("基础测试，姓名年龄") {
    val spark = SparkSession
      .builder
      .appName("Spark Pi")
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    // 定义定点,(id,(姓名,职业))
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // 定义边界,(id,id,关系)
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    // 计算postdoc的数量
    println(graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count)
    // 计算边界,from id 大于 to id的数量
    println(graph.edges.filter(e => e.srcId > e.dstId).count)



    // 遍历所有边界
    val facts: RDD[String] =
      graph.triplets.map(triplet =>
        triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    facts.collect.foreach(println(_))


  // inDegree , src -> dest , 指向dest的次数
    println("in degrees")
    graph.inDegrees.foreach(println _)

    // inDegree , src -> dest , src 出来的次数
    println("out degrees")
    graph.outDegrees.foreach(println _)


    //顶点的外连接操作
    val inputGraph: Graph[Int, String] =
      graph.outerJoinVertices(graph.outDegrees)((vid, _, degOpt) => degOpt.getOrElse(0))

    println("outerJoinVertices outDegrees")
    //在原来的边界关系基础上,给每个定点加上一个出度的属性
    inputGraph.triplets.collect().foreach(a => println(a))


    //
    val outputGraph: Graph[Double, Double] =
      inputGraph.mapTriplets(triplet => 1.0 / triplet.srcAttr).mapVertices((id, _) => 1.0)
    println("mapTriplets outDegrees")
    outputGraph.triplets.collect().foreach(println _)


    println("joinVertices outDegrees")
    graph.joinVertices(graph.outDegrees)((vid, b, degOpt) => b ).triplets.collect().foreach(println _)


    // 计算最大入度
    val maxInDegree: (VertexId, Int)  = graph.inDegrees.reduce((a,b) =>if (a._2 > b._2) a else b)

    println(maxInDegree)

    //汇总所有进入当前节点的所有节点
    graph.collectNeighborIds(EdgeDirection.In).collect().foreach( f=>{
      print(f._1 + "  :  ")
      f._2.foreach(println _)
      println("==")
    } )
    spark.stop()
  }

  test("基础年龄") {
    val spark = SparkSession
      .builder
      .appName("Spark Pi")
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val graph: Graph[Double, Int] =
      GraphGenerators.logNormalGraph(sc, numVertices = 100).mapVertices( (id, _) => id.toDouble )
    println("graph.triplets")

    graph.triplets.collect().foreach(println _)
    // Compute the number of older followers and their total age
    // 遍历所有顶点,计算每个顶点的所有边界from 大于to 的节点的 总数和总age
    val olderFollowers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](
      triplet => { // Map Function
        if (triplet.srcAttr > triplet.dstAttr) {
          // Send message to destination vertex containing counter and age
          triplet.sendToDst(1, triplet.srcAttr)
        }
      },
      // Add counter and age
      (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
    )

    println("olderFollowers")
    olderFollowers.collect().foreach(println _)

    // Divide total age by number of older followers to get average age of older followers
    val avgAgeOfOlderFollowers: VertexRDD[Double] =
      olderFollowers.mapValues( (id, value) =>
        value match { case (count, totalAge) => totalAge / count } )
    // Display the results
    println("avgAgeOfOlderFollowers")
    avgAgeOfOlderFollowers.collect.foreach(println(_))


    spark.stop()
  }


  test("基础x") {
    val spark = SparkSession
      .builder
      .appName("Spark Pi")
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )
    val edgeArray = Array(
      Edge(2L, 1L, 1),
      Edge(2L, 4L, 1),
      Edge(3L, 2L, 1),
      Edge(3L, 6L, 1),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 1),
      Edge(5L, 3L, 1),
      Edge(5L, 6L, 1)
    )
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD).subgraph(vpred = (id, user) => user._2 >= 28)

    // compute the connected components
    val cc = graph.connectedComponents
    println("compute the connected components")
    cc.triplets.collect().foreach(println _)

    // display the component id of each user:
    graph.vertices.leftJoin(cc.vertices) {
      case (id, user, comp) => s"${user._1} is in component ${comp.get}"
    }.collect.foreach{ case (id, str) => println(str) }

    // 最短路径计算 5到其他点
    val initialGraph = Graph(vertexRDD, edgeRDD).mapVertices((id, _) =>
      if (id == 5) 0.0 else Double.PositiveInfinity) // 在正式开始计算之前将源点到自己的路径长度设为0，到其它点的路径长度设为无穷大，如果遇到更短的路径替换当前的长度即可。如果源点到该点不可达，那么路径长度自然为无穷大了。

    println("initialGraph")
    initialGraph.triplets.collect().foreach(println _)

    /**
      * 第一个括号
      * initialMsg : 表示第一次迭代时即superstep 0，每个节点接收到的消息
      * maxIterations : 表示迭代的最大次数
      * activeDirection : 表示消息发送的方向
      *
      * 第二个括号中参数序列为三个函数，分别为vprog、sendMsg和mergeMsg。
      * vprog是节点上的用户定义的计算函数，运行在单个节点之上，在superstep 0，这个函数会在每个节点上以初始的initialMsg为参数运行并生成新的节点值。在随后的超步中只有当节点收到信息，该函数才会运行
      *
      * sendMsg在当前超步中收到信息的节点用于向相邻节点发送消息，这个消息用于下一个超步的计算
      *
      * mergeMsg用于聚合发送到同一节点的消息，这个函数的参数为两个A类型的消息，返回值为一个A类型的消息。
      */
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => {  // Send Message
        println(s"${triplet.srcId}[${triplet.srcAttr}] | ${triplet.attr}  | ${triplet.dstId}[${triplet.dstAttr}]")
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )

    println(sssp.vertices.collect.mkString("\n"))
  }
}
