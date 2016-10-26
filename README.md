#Eunomia Tree Example

This is copy of source code of Eunomia Tree with several examples to show the effectiveness of such a tree design.

##Implementation

Most implementation code of Eunomia Tree is in `memstore/memstore_eunotree.h`.
There is a copy of `memstore_eunotree.h` in the top directory.
All the functions related to pseudocode issues are explained in detail in the source file.

##Core Algorithm Implementations

###Section 3.2.1 Splitting HTM Region
In this section, we split the monolithic HTM scope into two parts: *upper region* and *lower region*.

Such design is implemented in `GetWithInsert()` function in `memstore/memstore_eunotree.h`.

###Section 3.2.2 Distributed Leaf Nodes
This section relates the partitioned leaf node structure, including the *write scheduler*, *segments*, and *reserved keys*.

Such design is implemented in `struct LeafNode` in `memstore/memstore_eunotree.h`.

###Section 3.2.3 Conflict Control Module
This section explains the data structure and mechanism of Conflict Control Module. 

Such design is implemented in `util/ccm.h`

###Section 3.3.1 Get/Put Interfaces
This section describes the Get/Put interfaces of Eunomia, including a piece of pseudocode (Algorithm 2).

Such algorithm is implemented in `GetWithInsert()` and `Get()` function in `memstore/memstore_eunotree.h`.

###Section 3.3.2 Insertions
This section elaborates the mechanism of inserting new keys in an existing leaf node, including a piece of pesudocode (Algorithm 3).

Such algorithm is implemented in `ShuffleLeafInsert()` function in `memstore/memstore_eunotree.h`.

###Section 3.3.3 Splits
This section illustrates the mechanism of spliting a full leaf node.

Such algorithm is implemented in `ShuffleLeafInsert()` and `ScopeInsert()` function in `memstore/memstore_eunotree.h`

##Experimental Setup in Our Paper

* CPU: 2.30GHz Intel(R) Xeon(R) E5-2650
* Cores: 2 sockets, each with 10 cores
* Cache: 32KB L1-I Cache, 32KB L1-D Cache, 256KB L2 Cache, 25MB L3 Cache
* DRAM: 256GB
* Workloads: Yahoo! Cloud Serving Benchmark (YCSB)

##Building Eunomia Tree with YCSB Workloads
    ./make_ycsb.sh

##Running Exmaples  
    cd ./script
    ./run_example.sh

