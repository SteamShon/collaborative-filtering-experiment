# This is my util library for experimenting with Machine learning I' ve learned from ML-class.org There were two main experiment I conducted for several weeks



* mahout`s collaborative filtering algorithm(als-wr) which is based on matrix factorization.
This is implementation for [Collaborative Filtering](http://research.yahoo.com/pub/2433) using Matrix factorization for implicit feedback. 
Heavily stolen from Mahout 0.7. if your matrix can be loaded into memory, then just use mahout.
when either fatorized matrix U or M is too large(> mapred.map.child.java.opts) then your child process may throw outofmemory exception.
There is obvious trade-off between speed and # of simultaneous map tasks on each datanode because mahout`s implementation load matrix into memory.
To make sure only certain number of map task runs simultaneosly on each datanode, I used hdfs file lock with busy waiting.

run 'RunALS.sh hdfs_input_path numIteration numFeature hdfs_output_path hdfs_tmp_path' to experiment with your data.
RunALS.sh will do following.

* parameters
1. hdfs_input_path: (user_id:string, item_id:string, rating:float) comma delimeted hdfs file.
2. numIteration: how many iterations you want to run ALS-WR. usually more iteration means better uptimization on cost function.
3. numFeatures: how many latent features you want to use. lager value of this means better uptimization on cost function.
4. alpha(coded in script): weight in trainform function for rating to confidence in paper.
5. lambda(coded in script): weight for regularization term on cost function. refer paper for detail.

* job flows
1. indexify your rating input: (user_index:integer, item_index:integer, rating:float).
2. split data per user_index. note mahout`s implementation doesn`t group by user_index for split.
3. factorize rating matrix into U/M. this will take a lot of times depends on user matrix dimension.
4. build recommendations using U x M.
5. remove already rated items in recommendations.
6. run evaluation(optional).
7. de-indexify recommendations result.

* random walk on bipartitie graph using mahout`s distributed matrix library. 


