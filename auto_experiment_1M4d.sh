#!/bin/bash

sudo -u hdfs spark-submit --class org.miudit.spark.mllib.clustering.OpticsDriver --master yarn --deploy-mode cluster --executor-cores 1 --name OPTICS_nomerge_1M4d_4 target/scala-2.10/spark_optics_2.10-0.0.1.jar data_1M_4d.csv 10.0 3 4 4
sudo -u hdfs spark-submit --class org.miudit.spark.mllib.clustering.OpticsDriver --master yarn --deploy-mode cluster --executor-cores 1 --name OPTICS_nomerge_1M4d_8 target/scala-2.10/spark_optics_2.10-0.0.1.jar data_1M_4d.csv 10.0 3 8 4
sudo -u hdfs spark-submit --class org.miudit.spark.mllib.clustering.OpticsDriver --master yarn --deploy-mode cluster --executor-cores 1 --name OPTICS_nomerge_1M4d_16 target/scala-2.10/spark_optics_2.10-0.0.1.jar data_1M_4d.csv 10.0 3 16 4
sudo -u hdfs spark-submit --class org.miudit.spark.mllib.clustering.OpticsDriver --master yarn --deploy-mode cluster --executor-cores 1 --name OPTICS_nomerge_1M4d_32 target/scala-2.10/spark_optics_2.10-0.0.1.jar data_1M_4d.csv 10.0 3 32 4

