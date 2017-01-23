package org.miudit.spark.mllib

import scala.collection.mutable.{ArrayBuffer, PriorityQueue}

package object clustering {

    type PartialClusterOrderingId = Int

    type ClusterOrdering = ArrayBuffer[MutablePoint]

    type AccumlableType = (Int, (ClusterOrdering, Box))

}
