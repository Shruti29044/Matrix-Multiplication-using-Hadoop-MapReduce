📘 Matrix Multiplication using Hadoop MapReduce

This project implements distributed matrix multiplication using the Hadoop MapReduce framework in Java. It processes two matrices M and N, and outputs the result matrix P = M × N using two MapReduce jobs.

💡 Overview

Implements parallel matrix multiplication using MapReduce

Handles large matrices by distributing the computation across Hadoop nodes

Uses custom Writable classes (Elem, Pair)

First MapReduce job computes partial products

Second MapReduce job aggregates them into final matrix entries

How It Works

1️⃣ First MapReduce Job

Two Mappers:

MMatriceMapper: Emits matrix M's rows

NMatriceMapper: Emits matrix N's columns

Reducer:

Joins M and N on shared dimension k

Computes all M[i][k] * N[k][j]

Emits key-value pairs (i,j) → partial product

2️⃣ Second MapReduce Job

Mapper:

Reads partial products from first job

Reducer:

Sums all partial products for each (i,j) to get final P[i][j]

Technologies Used:

Java

Hadoop MapReduce API

Custom Writable/Comparable for complex keys and values

Challenges Faced:

Designing custom Writable classes for matrix elements and index pairs

Managing data flow between two MapReduce jobs

Debugging nested loops and ensuring correctness of key emission

Handling duplicates or missing elements in sparse matrices

