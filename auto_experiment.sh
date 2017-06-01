#!/bin/bash

sudo -u hdfs spark-submit --class org.miudit.spark.mllib.clustering.OpticsDriver --master yarn --deploy-mode cluster --executor-cores 1 --name OPTICS_nomerge_10K2d_1 target/scala-2.10/spark_optics_2.10-0.0.1.jar data_10K_2d.csv 2.0 3 1 4
sudo -u hdfs spark-submit --class org.miudit.spark.mllib.clustering.OpticsDriver --master yarn --deploy-mode cluster --executor-cores 1 --name OPTICS_nomerge_10K2d_1 target/scala-2.10/spark_optics_2.10-0.0.1.jar data_10K_2d.csv 2.0 3 1 4
sudo -u hdfs spark-submit --class org.miudit.spark.mllib.clustering.OpticsDriver --master yarn --deploy-mode cluster --executor-cores 1 --name OPTICS_nomerge_10K2d_1 target/scala-2.10/spark_optics_2.10-0.0.1.jar data_10K_2d.csv 2.0 3 1 4
sudo -u hdfs spark-submit --class org.miudit.spark.mllib.clustering.OpticsDriver --master yarn --deploy-mode cluster --executor-cores 1 --name OPTICS_nomerge_10K2d_2 target/scala-2.10/spark_optics_2.10-0.0.1.jar data_10K_2d.csv 2.0 3 2 4
sudo -u hdfs spark-submit --class org.miudit.spark.mllib.clustering.OpticsDriver --master yarn --deploy-mode cluster --executor-cores 1 --name OPTICS_nomerge_10K2d_2 target/scala-2.10/spark_optics_2.10-0.0.1.jar data_10K_2d.csv 2.0 3 2 4
sudo -u hdfs spark-submit --class org.miudit.spark.mllib.clustering.OpticsDriver --master yarn --deploy-mode cluster --executor-cores 1 --name OPTICS_nomerge_10K2d_2 target/scala-2.10/spark_optics_2.10-0.0.1.jar data_10K_2d.csv 2.0 3 2 4
