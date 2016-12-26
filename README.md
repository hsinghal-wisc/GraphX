Implement PageRank using Spark GraphX and then run some queries on it.

(1)Part-B Application-1 Question-1

Initially we associate degree with each vertices using join operation, based on degree value each edge attribute
his initialized with weight 1/outdegree and vertex attributes are initialized with initial page rank values.
PageRank stored as vertex attributes is updated using aggregate messages for 20 iterations.

(2)Part-B Application-2 Question-1

VertexRDD is constructed with interval number as vertexId and top-words in the interval as attributes. EdgeRDD
is created filtering cartesian of vertexRDD with itself, filtering criteria is both vertices in cartesian should
have at least one common attribute and their id’s should not be same (to remove self loop) . Further, we create
the graph using VertexRDD and EdgeRDD. 

Then we calculate number of edges having number of words in the source vertex strictly larger than the number of
words in the destination vertex by applying filter on triplet view of graph based on filtering criteria that
size of source vertex attribute should be greater than that of destination vertex.

(3)Part-B Application-2 Question-2

VertexRDD is constructed with interval number as vertexId and top-words in the interval as attributes. EdgeRDD
is created filtering cartesian of vertexRDD with itself, filtering criteria is both vertices in cartesian should
have at least one common attribute and their id’s should not be same (to remove self loop) . Further, we create
the graph using VertexRDD and EdgeRDD. 

Then we find list of vertices that have the maximum number of edges if number of such vertices is 1 then the
vertex is most popular vertex. If number of vertices having maximum out degree is more than we create a
vertexRDD by join operation on vertices having maximum outdegree with vertices on the graph. Resultant vertexRDD
is sorted based on size of attribute set. Sorted RDD is collected in an Array of vertices, vertex at end of the
array is most popular vertex.

(4)Part-B Application-2 Question-3

VertexRDD is constructed with interval number as vertexId and top-words in the interval as attributes. EdgeRDD
is created filtering cartesian of vertexRDD with itself, filtering criteria is both vertices in cartesian should
have at least one common attribute and their id’s should not be same (to remove self loop) . Further, we create
the graph using VertexRDD and EdgeRDD. 

Then we calculate average number of words in neighbour vertices by calculating total number of words in
neighbour vertices and count of neighbour vertices using aggregateMessage. Ratio of total number of vertices in
neighbour to count of vertices is required average number of words in neighbour vertices.


(5)Part-B Extra Credit-1

VertexRDD is constructed with interval number as vertexId and top-words in the interval as attributes. EdgeRDD
is created filtering cartesian of vertexRDD with itself, filtering criteria is both vertices in cartesian should
have at least one common attribute and their id’s should not be same (to remove self loop) . Further, we create
the graph using VertexRDD and EdgeRDD. 


(6)Part-B Extra Credit-2

VertexRDD is constructed with interval number as vertexId and top-words in the interval as attributes. EdgeRDD
is created filtering cartesian of vertexRDD with itself, filtering criteria is both vertices in cartesian should
have at least one common attribute and their id’s should not be same (to remove self loop) . Further, we create
the graph using VertexRDD and EdgeRDD. 


(7)Part-B Extra Credit-3

VertexRDD is constructed with interval number as vertexId and top-words in the interval as attributes. EdgeRDD
is created filtering cartesian of vertexRDD with itself, filtering criteria is both vertices in cartesian should
have at least one common attribute and their id’s should not be same (to remove self loop) . Further, we create
the graph using VertexRDD and EdgeRDD. 
