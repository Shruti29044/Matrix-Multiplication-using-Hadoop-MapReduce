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

🔧 Setup & Compilation

🔹 Unzip the project using: unzip MatMult.zip

🔹 Navigate into the project: cd MatMult

🔹 Ensure Hadoop is installed and configured (hadoop version)

🔹 Compile the Java code with Hadoop classpath:

javac -classpath `hadoop classpath` -d . Multiply.java

🔹 Package the compiled classes into a JAR:

jar -cvf matmult.jar *.class

📁 Prepare Input Data

🔹 Create input files MatrixM.txt and MatrixN.txt in the format:

📤 Upload to HDFS

🔹 Create an HDFS input directory:

hdfs dfs -mkdir -p /input

🔹 Upload matrix files: 

hdfs dfs -put MatrixM.txt /input/M
hdfs dfs -put MatrixN.txt /input/N

🚀 Run MapReduce Jobs

🔹 First job (generates intermediate output):

hadoop jar matmult.jar Multiply /input/M /input/N /intermediate

🔹 Second job (final matrix output):

hadoop jar matmult.jar Multiply /intermediate /output

📄 View the Result

🔹 Check result from HDFS:

hdfs dfs -cat /output/part-r-00000

🧹 Clean Up (Optional)

🔹 Remove old HDFS outputs if rerunning:

hdfs dfs -rm -r /intermediate /output




