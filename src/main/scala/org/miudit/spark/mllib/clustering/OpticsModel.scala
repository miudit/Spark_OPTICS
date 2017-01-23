package org.miudit.spark.mllib.clustering

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.util.{Loader, Saveable}

class OpticsModel (
    val result: (ClusterOrdering, Int),
    val epsilon: Double,
    val minPts: Int ) extends Serializable {



}
