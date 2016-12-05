package org.miudit.spark.mllib.clustering

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.util.{Loader, Saveable}

class OpticsModel (
    val points: RDD[Point],
    val epsilon: Double,
    val minPts: Int ) extends Saveable with Serializable {



}
