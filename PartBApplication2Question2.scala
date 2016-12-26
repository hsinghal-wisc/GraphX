import scala.language.postfixOps
import scala.reflect.ClassTag
import org.apache.spark.graphx._
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner
import org.apache.spark.RangePartitioner
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.EdgeRDD
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.internal.Logging
import org.apache.spark.graphx.impl.EdgePartitionBuilder
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.util.collection.{PrimitiveVector, SortDataFormat, Sorter}
import org.apache.spark.graphx
import org.apache.spark.graphx.impl.{EdgePartitionBuilder, GraphImpl}



import scala.language.implicitConversions

import org.apache.spark.graphx.impl._

object PartBApplication2Question2 {
  def main(args: Array[String]) {

    val conf  = new SparkConf()
      .set("spark.executor.memory", "4G")
      .set("spark.driver.memory", "1G")
      .set("spark.executor.cores","4")
      .set("spark.task.cpus","1")
      .set("spark.eventLog.enabled","true")
      .set("spark.eventLog.dir","hdfs://10.254.0.178/logs")
      .set("spark.executor.instances","5")
      .setMaster("spark://10.254.0.178:7077")

    val spark = SparkSession
      .builder.config(conf=conf)
      .appName("CS-838-Assignment3-PartB-1")
      .getOrCreate()

    val sc = spark.sparkContext

    //val iters = args(1).toInt

    val users = (sc.textFile("topwords.txt").map(line => line.split(" ")).map( parts => (parts.head.toLong, parts.tail.toSet) ))

    // output: Array[(Long, Array[String])] =
    val output = users.collect
    output.foreach(tup => println(tup._1 + " has words : " + tup._2.foreach (word => println(word)) ))

    // cartesian_users: org.apache.spark.rdd.RDD[((Long, Array[String]), (Long, Array[String]))] = CartesianRDD[4]
    val cartesian_users  = users.cartesian(users)

    //output2: Array[((Long, Array[String]), (Long, Array[String]))] =
    //    val output2 = cartesian_users.collect
    val filteredEdgeRDD = cartesian_users.filter{case(a:(Long,Set[String]), b:(Long,Set[String])) => (a._2 & b._2).size > 0 && a._1 != b._1 }

    val EdgeRDD = filteredEdgeRDD.map{case(a:(Long,Set[String]), b:(Long,Set[String])) => Edge(a._1, b._1, 1.0)}

    // print all pair of vertices
    //    output2.foreach(tup =>  println( tup._1._1 + " " + tup._2._1  ) )
    // print all possible attribute set corresponding to vertex pairs
    //    output2.foreach(tup =>  println( tup._1._2.toSet & tup._2._2.toSet  ) )

    // print true if pair of sets contains at least one common word
    // true implies edge exists as stated in assignment question
    //    output2.foreach(tup =>  println( (tup._1._2.toSet & tup._2._2.toSet).size > 0  ) )

    // we should remove self-loop on each vertex due to cartesian
    // i.e tup._1._1 != tup._2._1
    // print true after removing self loops if edge exists between pair of vertices
    //    output2.foreach(tup =>  println(  (tup._1._1 != tup._2._1) && (tup._1._2.toSet & tup._2._2.toSet).size > 0  ) )


    //relationships: Array[((Long, Array[String]), (Long, Array[String]))] =
    // val relationships = output2.filter{ case (tup) => (tup._1._1 != tup._2._1) && (tup._1._2.toSet & tup._2._2.toSet).size > 0 }

    // Define a default user in case there are relationship with missing user
    val defaultUser = (" ", " ")
    // Build the initial Graph
    //relationships.
    val graph: Graph[Set[String], Double] = Graph(users,EdgeRDD)
    graph.vertices.collect.foreach(println)
    graph.edges.collect.foreach(println)
    println("******************************************************************************")
    println("GRAPHX: Number of vertices " + graph.vertices.count)
    println("GRAPHX: Number of edges " + graph.edges.count)
    println(s"The graph has ${graph.numEdges} edges")
    println(s"The graph has ${graph.numVertices} vertices")
    println("##############################################################################")
     
     // Define a reduce operation to compute the highest degree vertex
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2 ) a else b
    }
    // Compute the max outdegree
    val maxOutDegree: (VertexId, Int) = graph.outDegrees.reduce(max)
    println("GRAPHX: Maximum outdegree is  " + maxOutDegree)

    //1. Find vertices that have the maximum number of edges.Let's say we have a set of X vertices.
   // graph.outDegrees.collect.sortWith(_._2 > _._2)
    val verticesWithMaxOutDegree =  graph.outDegrees.filter{ case(a) =>  a._2==maxOutDegree._2 }
    verticesWithMaxOutDegree.foreach(println(_))

    val noVerticesMaxOutDegree = verticesWithMaxOutDegree.collect.size

    // 2. Incase we have X=1, we stop.
    if(noVerticesMaxOutDegree == 1 ) {
      println("Most popular vertex: " +maxOutDegree._1 + " " )
    } else {
      /* 2. If X > 1, among the X vertices, choose the one which has the maximum number of words (in that vertex or interval).
       Let's say we now have a set of Y vertices.
       */
      println("Following vertices are candidate for most popular vertices <VertexId, Outdegree >")
      verticesWithMaxOutDegree.foreach(println(_))
      val resultGraph = graph.vertices
      val result = resultGraph.join(verticesWithMaxOutDegree)

      println("Set size are : ")

     // graph.join(verticesWithMaxOutDegree).vertices.filter()
     // graph.vertices.join(verticesWithMaxOutDegree)
      //graph.vertices.filter { case (ver) => ver.VertexId == maxOutDegree._1 }
      //val verticesWithMaxWords = verticesWithMaxOutDegree.filter{  case(a) =>  a._1.size >=maxOutDegree._1.size }
      //verticesWithMaxWords.foreach(println(_))
      //val verticesWithMaxWords = graph.triplets.filter{ triplet => triplet.srcAttr.size > triplet.dstAttr.size}
       //val currentMaxWords = graph.triplets.filter(triplet => triplet.srcId==maxOutDegree._1 )
      //val subgraph = graph.outerJoinVertices(verticesWithMaxOutDegree) {
       // case (uid, deg, Some(attrList)) => attrList
     // }
      // Restrict the graph to users with usernames and names
      //val subgraph = graph.subgraph(vpred = (vid, attr) => attr.size == 2)
    }

    //graph.vertices.collect.foreach( )
    //val count_temp = graph.triplets.filter(triplet => triplet.srcAttr.size > triplet.dstAttr.size).count()
    //var mostPopularVertexId : (VertexId, Int)  ;
     /*
    def mostPopularVertex( a:Int, b:Int ) : VertexId = {

      graph.vertices.map( triplet => if( triplet.srcAttr.size > maxOutDegree._2 )
      var sum:Int = 0
      sum = a + b
      return sum
    }


    val popularVertices: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](
      triplet => { // Map Function
        //if (triplet.srcAttr > triplet.dstAttr) {
          // Send message to destination vertex containing counter and age
          triplet.sendToSrc(1, triplet.srcAttr.size)
        //}
      },
      // Add counter and age
      (a, b) => (a._1 + b._1, a._2) // Reduce Function
    )

    //val count_temp = graph.triplets.filter(triplet => triplet.srcAttr.size > triplet.dstAttr.size).count()
    // Divide total age by number of older followers to get average age of older followers
    val mostPopularVertex: VertexRDD[Double] =
      popularVertices.mapValues( (id, value) =>
        value match { case (count, totalAge) => totalAge / count } )
    // Display the results
    mostPopularVertex.collect.foreach(println(_))
    */

   // val temp = graph.vertices.filter(e => e.attr.size < e.attr.size).count // count number of vertices having outdegree > indegree
    //println("total vertices having more topwords  " + temp)
    val countFollowers: VertexRDD[(Int)] = graph.aggregateMessages[(Int)](
      triplet => { // Map Function
        triplet.sendToDst(1)
      },
      // Add counter
      (a,b) => (a+b) // Reduce Function
    )
    val getCountFollowers: VertexRDD[Boolean] =
      countFollowers.mapValues( (id, value) =>
        value match { case (count) => count ==maxOutDegree._2  } )
    // Display the results
    getCountFollowers.collect.foreach(println(_))

    sc.stop()
  }
}