# This is my util library for experimenting with Machine learning I' ve learned from ML-class.org.
# There were two main experiment I conducted for several weeks. 



### Collaborative filtering algorithm(als-wr) which is based on matrix factorization(from mahout).
This is implementation for [Collaborative Filtering](http://research.yahoo.com/pub/2433) using Matrix factorization for implicit feedback. 

*Heavily stolen from Mahout 0.7. if your matrix can be loaded into memory, then just use mahout.*

When either fatorized matrix U or M is too large(larger than mapred.map.child.java.opts), then your child process may throw OutOfMemory exception.

There is obvious trade-off between speed and # of simultaneous map tasks on each datanode because mahout implementation load matrix into memory.
To make sure only certain number of map task runs simultaneosly on each datanode, I used hdfs file lock with busy waiting.

run 'RunALS.sh hdfs-input-path numIteration numFeature hdfs-output-path hdfs-tmp-path' to experiment with your data.
RunALS.sh will do following.

#### Parameters
1. hdfs-input-path: (user-id:string, item-id:string, rating:float) comma delimeted hdfs file.
2. numIteration: how many iterations you want to run ALS-WR. usually more iteration means better uptimization on cost function.
3. numFeatures: how many latent features you want to use. lager value of this means better uptimization on cost function.
4. alpha(coded in script): weight in trainform function for rating to confidence in paper.
5. lambda(coded in script): weight for regularization term on cost function. refer paper for detail.

#### Job flows
1. indexify your rating input: (user-index:integer, item-index:integer, rating:float).
2. split data per user-index. note mahout`s implementation doesn`t group by user-index for split.
3. factorize rating matrix into U/M. this will take a lot of times depends on user matrix dimension.
4. build recommendations using U x M.
5. remove already rated items in recommendations.
6. run evaluation(optional).
7. de-indexify recommendations result.

### Random walk on bipartitie graph using mahout math library.
This is implementation for [RandomWalk Algorithm](http://www2008.org/papers/pdf/p61-fuxmanA.pdf) on bipartite indirect graph using mahout math library.

*If your graph is bipartie use this*

run 'RunARW.sh hdfs-input-path hdfs-output-path hdfs-tmp-path' to try.

#### Parameters
1. hdfs-input-path: (source-id:string, target-id:string, weight:float) comma seperated hdfs file.
2. hdfs-output-path: output path.
3. tmp: temp path.
4. sinkProb: sink threshold to consider as 0. large value of this will cause too much network data transfer and will lead can`t fork copy on each datanode. too small value of this will not discover any indirect link.
5. iteration: how many times propagate probability. theoretically after many iteration this algorithm should converge.

#### Concepts
Concept is composite of vertices in input graph that represent abstract themes.
In simplist form, Concept can be one vertice in graph of set of vertices. 

ex) assumes bipartite graph from click-log. left vertices are queries and right vertices are URL that have been clicked by queries.
in this case concept can be following. 

1. each URL and we will get probabilities of every queries that will click this concept URL.
2. set of URLs that represent bigger concept like category. 
3. composite of 1 and 2.

currently I only implemented simplist concept 1. override retrieveClassMatrix if you want others.

#### Results 
1. probability matrix(|left vertices| x |concepts|). output can be considered as likelyhood that every left vertices will sink into each concept.
2. probability matrix(|right vertices| x |concepts|). note that if we initiate concept on right vertices, probability matrix(|right vertices| x |concepts|) can be considered as soft-clustering.

