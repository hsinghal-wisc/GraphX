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
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds


object PartBApplication1Question1 {
  def main(args: Array[String]) {
    val conf  = new SparkConf()
      .set("spark.executor.memory", "16G") //4G
      .set("spark.driver.memory", "4G")
      .set("spark.executor.cores","4")
      .set("spark.task.cpus","1")
      .set("spark.eventLog.enabled","true")
      .set("spark.eventLog.dir","hdfs://10.254.0.178/logs")
      //.set("spark.executor.instances","5") //5
      .setMaster("spark://10.254.0.178:7077")
    val spark = SparkSession
      .builder.config(conf=conf)
      .appName("CS-838-Assignment3-PartB-1")
      .getOrCreate()
    val sc = spark.sparkContext
    //val iters = args(1).toInt
    // Load the edges as a graph
    val graph = GraphLoader.edgeListFile(sc, "soc-LiveJournal1.txt") ///Users/gautam/Desktop/soc-LiveJournal1.txt
    val parts = 60//args(1).toInt

    println("******************************************************************************")
    // Run PageRank
    var start_time = System.currentTimeMillis()
    val ranks = run(graph, 20)
    //var end_time = System.currentTimeMillis()

    println("******************************************************************************")
    println("GRAPHX: Number of vertices " + graph.vertices.count)
    println("GRAPHX: Number of edges " + graph.edges.count)
    println(s"The graph has ${graph.numEdges} edges")
    println(s"The graph has ${graph.numVertices} vertices")
    println("##############################################################################")

    //val output = ranks.collect()
    //ranks.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
    //println(ranks.collect().mkString("\n"))

    val output = ranks.vertices.collect
    output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
    var end_time = System.currentTimeMillis()
    println("##############################################################################")

    println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<   >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    var pagerank_execution_time = end_time - start_time
    println("PageRank execution time in miliseconds is " + pagerank_execution_time)
    println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<   >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    spark.stop()
  }



def run[VD: ClassTag, ED: ClassTag] (graph: Graph[VD, ED], numIter: Int, resetProb: Double = 0.15): Graph[Double, Double] =
{
  runWithOptions(graph, numIter, resetProb)
}
def runWithOptions[VD: ClassTag, ED: ClassTag](
                                                graph: Graph[VD, ED], numIter: Int, resetProb: Double = 0.15,
                                                srcId: Option[VertexId] = None): Graph[Double, Double] =
{
  require(numIter > 0, s"Number of iterations must be greater than 0," +
    s" but got ${numIter}")
  require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong" +
    s" to [0, 1], but got ${resetProb}")

  val personalized = srcId isDefined
  val src: VertexId = srcId.getOrElse(-1L)

  // Initialize the PageRank graph with each edge attribute having
  // weight 1/outDegree and each vertex with attribute resetProb.
  // When running personalized pagerank, only the source vertex
  // has an attribute resetProb. All others are set to 0.
  var rankGraph: Graph[Double, Double] = graph
    // Associate the degree with each vertex
    .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
    // Set the weight on the edges based on the degree
    .mapTriplets( e => 1.0 / e.srcAttr, TripletFields.Src )
    // Set the vertex attributes to the initial pagerank values
    .mapVertices { (id, attr) =>
    if (!(id != src && personalized)) resetProb else 0.0
  }
  def delta(u: VertexId, v: VertexId): Double = { if (u == v) 1.0 else 0.0 }
  var iteration = 0
  var prevRankGraph: Graph[Double, Double] = null
  while (iteration < numIter) {
    rankGraph.cache()
    val rankUpdates = rankGraph.aggregateMessages[Double](
      ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr), _ + _, TripletFields.Src)
    prevRankGraph = rankGraph
    val rPrb = if (personalized) {
      (src: VertexId, id: VertexId) => resetProb * delta(src, id)
    } else {
      (src: VertexId, id: VertexId) => resetProb
    }
    rankGraph = rankGraph.joinVertices(rankUpdates) {
      (id, oldRank, msgSum) => rPrb(src, id) + (1.0 - resetProb) * msgSum
    }.cache()
    rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
    prevRankGraph.vertices.unpersist(false)
    prevRankGraph.edges.unpersist(false)
    iteration += 1
  }
  rankGraph
}
}