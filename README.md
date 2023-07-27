# Higgs bosons prediction with PySpark

## Dataset
https://archive.ics.uci.edu/dataset/280/higgs

## Objective

The project consisit in the application of **3** between **classification and clustering algorithms**:

* Decision Tree Classifier
* Random Forest Classifier
* K-Means Clustering
  
to the original dataset and some meaningful variations of it:

* original dataset (with dropped NaNs)
* dataset with only features selected by the variance
* dataset with only function columns
* dataset with only kinematic features columns
  
The experiment rely on **PySpark** and **HDFS**.

## Setup
The experiment is setup on a hdfs cluster with 3 nodes:
* node-master
* node1
* node2

and Spark relying on the data stored in the HDFS.
The experiment is also performed on a single node.