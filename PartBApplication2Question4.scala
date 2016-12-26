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
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.internal.Logging
import org.apache.spark.graphx.impl.EdgePartitionBuilder
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.util.collection.{PrimitiveVector, SortDataFormat, Sorter}
import org.apache.spark.graphx
import org.apache.spark.graphx.impl.{EdgePartitionBuilder, GraphImpl}
import scala.language.implicitConversions
import org.apache.spark.graphx.impl._

object PartBApplication2Question4 {
  def main(args: Array[String]) {

    val conf  = new SparkConf()
     // .setMaster("local[1]")
     // .setAppName("test1")
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
      .appName("CS-838-Assignment3-PartB-4")
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

    val EdgeRDD = filteredEdgeRDD.map{case(a:(Long,Set[String]), b:(Long,Set[String])) => Edge(a._1, b._1, (a._2 & b._2))}

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

    val graph: Graph[Set[String], Set[String]] = Graph(users,EdgeRDD)
    graph.vertices.collect.foreach(println)
    graph.edges.collect.foreach(println)
    println("******************************************************************************")
    println("GRAPHX: Number of vertices " + graph.vertices.count)
    println("GRAPHX: Number of edges " + graph.edges.count)
    println(s"The graph has ${graph.numEdges} edges")
    println(s"The graph has ${graph.numVertices} vertices")
    println("##############################################################################")


    sc.stop()
  }
}
