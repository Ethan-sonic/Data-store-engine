# Key-Value Storage Engine in Go

This project implements a **key-value engine** for storing large amounts of data, inspired by the principles of modern databases. The project was developed as a team assignment at university, and my contribution includes the implementation and optimization of key data structures and algorithms.

## Features
- Storage of data in the form of **key-value pairs**
- Multi-layer architecture: cache, memory, summary structures, disk
- Efficient data lookup across different layers
- **Compaction** of data when SStable files grow too large
- Implementation of probabilistic algorithms for data analysis

## Data Structures and Algorithms
- **Bloom filter** – fast check whether a key exists
- **Cache** – layer for frequently accessed data
- **Memtable (skip-list)** – in-memory storage before writing to disk
- **Write Ahead Log (WAL)** – ensures durability before data is moved to memtable
- **SStable (Sorted String Table)** – persistent storage on disk
- **Compaction** – merging and optimizing SStable files to reduce space and improve search performance
- **Count-Min Sketch** – probabilistic algorithm for estimating element frequency
- **HyperLogLog** – algorithm for estimating the number of distinct elements (cardinality estimation)

## Author's Note
This project was created as a team assignment at university. My contribution includes the implementation and optimization of key data structures (skip-list, Bloom filter, compaction) and probabilistic algorithms (Count-Min Sketch, HyperLogLog). The project demonstrates principles on which modern databases and data storage systems are based.
