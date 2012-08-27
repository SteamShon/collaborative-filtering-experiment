# This is my util library for experimenting with Machine learning I' ve learned from ML-class.org. There were two main experiment I conducted for several weeks. 


* * *
## Collaborative filtering algorithm(als-wr) which is based on matrix factorization(from mahout).
This is multithreaded implementation for [Collaborative Filtering](http://research.yahoo.com/pub/2433)(ALS-WR) using Matrix factorization for implicit feedback. 

*Heavily stolen from Mahout 0.7. if your matrix can be loaded into memory, then just use mahout.*

### Why re-implement algorithm in mahout? problems were follwing.
1. U/M matrix is too large(about 8G in 2D float array) for mapred.map.child.java.opts. mahout implementation runs out of heap space when load U/M matrix into memory. I used hdfs file lock and fair scheduler to force not too many map task runs simultaneously on same datanode.
2. loading U/M matrix into memory for each setup is single threaded. since keys in each part hdfs files are mutual exclusive(key is row id), it is safe to run multiple thread to read multiple part hdfs file into memory.
3. even though U/M is loaded into memory, single thread to calculate current ui, mj is too slow. note that each ui, mj can be run simultaneously using multiple thread. I added multithreadedMapMapper to span multithreaded map method.

using multithread in map task, it runs 3 times(used 10 thread per task) fater than single threaded. 
Still need to figure out how to emit asyncronously in mapper.

* * *
### Run
	RunALS.sh hdfs-input-path numIteration numFeature hdfs-output-path hdfs-tmp-path 

* * *
### Parameters
1. hdfs-input-path: (user-id:string, item-id:string, rating:float) comma delimeted hdfs file.

2. numIteration: how many iterations you want to run ALS-WR. usually more iteration means better uptimization on cost function.

3. numFeatures: how many latent features you want to use. lager value of this means better uptimization on cost function.

4. alpha(coded in script): weight in trainform function for rating to confidence in paper.

5. lambda(coded in script): weight for regularization term on cost function. refer paper for detail.

* * *
### Job flows
1. indexify your rating input: (user-index:integer, item-index:integer, rating:float).

2. split data per user-index. note mahout`s implementation doesn`t group by user-index for split.

3. factorize rating matrix into U/M. this will take a lot of times depends on user matrix dimension.

4. build recommendations using U x M.

5. remove already rated items in recommendations.

6. run evaluation(optional).

7. de-indexify recommendations result.

* * *
### Results
1. recommendations per each user-id.

* * * 
## Random walk on bipartitie graph using mahout math library.
This is implementation for [RandomWalk Algorithm](http://www2008.org/papers/pdf/p61-fuxmanA.pdf) on bipartite indirect graph using mahout math library.

*If your graph is bipartie use this*

* * *
### Concepts
Concept is composite of vertices in input graph that represent abstract themes.
In simplist form, Concept can be one vertice in graph of set of vertices. 

ex) assumes bipartite graph from click-log. left vertices are queries and right vertices are URL that have been clicked by queries.
in this case concept can be following. 

1. each URL and we will get probabilities of every queries that will click this concept URL.

2. set of URLs that represent bigger concept like category. 

3. composite of 1 and 2.

currently I only implemented simplist concept 1. override retrieveClassMatrix if you want others.

### Run
	RunARW.sh hdfs-input-path hdfs-output-path hdfs-tmp-path

### Parameters
1. hdfs-input-path: (source-id:string, target-id:string, weight:float) comma seperated hdfs file.

2. hdfs-output-path: output path.

3. tmp: temp path.

4. sinkProb: sink threshold to consider as 0. large value of this will cause too much network data transfer and will lead can`t fork copy on each datanode. too small value of this will not discover any indirect link.

5. iteration: how many times propagate probability. theoretically after many iteration this algorithm should converge.

### Results 
1. probability matrix(|left vertices| x |concepts|). output can be considered as likelyhood that every left vertices will sink into each concept.

2. probability matrix(|right vertices| x |concepts|). note that if we initiate concept on right vertices, probability matrix(|right vertices| x |concepts|) can be considered as soft-clustering.

