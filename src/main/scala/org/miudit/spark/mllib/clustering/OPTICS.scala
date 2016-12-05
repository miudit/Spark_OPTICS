package org.miudit.spark.mllib.clustering

import scala.collection.mutable.{ArrayBuffer, PriorityQueue}
import org.apache.commons.math3.ml.distance.{DistanceMeasure, EuclideanDistance}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}

class Optics private (
    private var epsilon: Double,
    private var minPts: Int ) extends Serializable with Logging {

    def this() = this(1, 1)

    def getEpsilon: Double = epsilon

    def setEpsilon(epsilon: Double): this.type = {
        require(epsilon >= 0, s"epsilon must be nonnegative")
        this.epsilon = epsilon
        this
    }

    def getMinPts: Int = minPts

    def setMinPts(minPts: Int): this.type = {
        require(minPts >= 0, s"minPts must be nonnegative")
        this.minPts = minPts
        this
    }

    private[spark] def run (data: RDD[Array[Double]]): OpticsModel = {
        var points = data.map{p => new Point(p)}
        val partitionedData = PointPartitions(points, epsilon, minPts)

        val broadcastBoxes = data.sparkContext.broadcast(partitionedData.boxes)

        val partialClusters = partitionedData.mapPartitionsWithIndex (
            (partitionIndex, it) => {
                val boxes = broadcastBoxes.value
                val partitionBoundingBox = boxes.find(  _.partitionId == partitionIndex ).get
                partialClustering(it, partitionBoundingBox).toIterator
            },
            preservesPartitioning = true
        )

        val mergedClusters = mergeClusters(partialClusters, partitionedData.boxes)

        new OpticsModel(mergedClusters, epsilon, minPts)
    }

    private def partialClustering (
        it: Iterator[(PointSortKey, Point)],
        boundingBox: Box ): ClusterOrdering = {

        var tempPointId: Long = 0
        var points = it.map {
            x => {
                tempPointId += 1
                var newPt = new MutablePoint(x._2, tempPointId)

                (tempPointId, newPt)
            }
        }.toMap

        var partitionIndexer = new PartitionIndexer(boundingBox, points.values, epsilon, minPts)
        var priorityQueue = new PriorityQueue[MutablePoint]()(Ordering.by[MutablePoint, Double](_.reachDist.get).reverse)
        var clusterOrdering = new ClusterOrdering

        var tempPoint: Option[MutablePoint] = None
        while ( { tempPoint = findOneUnprocessedPoint(points.values); tempPoint.isDefined } ) {
            var point = tempPoint.get
            expand(point, points, partitionIndexer, priorityQueue, clusterOrdering)
        }

        points.values
        .find( p => p.noise )
        .map( p => clusterOrdering.append(p) )

        clusterOrdering
    }

    private def mergeClusters (
        partialClusters: RDD[MutablePoint],
        boxes: Iterable[Box] ): RDD[Point] = {

        /*partialClusters.treeAggregate()(
            seqOp: (org.apache.spark.rdd.RDD.U, org.miudit.spark.mllib.clustering.MutablePoint) => org.apache.spark.rdd.RDD.U,
            combOp: (org.apache.spark.rdd.RDD.U, org.apache.spark.rdd.RDD.U) => org.apache.spark.rdd.RDD.U,
            BoxCalculator.maxTreeLevel
        )*/

        while (partialClusters.getNumPartitions > 1) {
            partialClusters.mapPartitionsWithIndex(
                (index, iterator) => {  }
            )
        }

        None
    }

    private def merge (
        co1: ClusterOrdering,
        co2: ClusterOrdering ): ClusterOrdering = {

        new ClusterOrdering()
    }

    private def expand (
        startPoint: MutablePoint,
        points: Map[Long, MutablePoint],
        partitionIndexer: PartitionIndexer,
        priorityQueue: PriorityQueue[MutablePoint],
        clusterOrdering: ClusterOrdering ): Unit = {

        var neighbors = partitionIndexer.findNeighbors(startPoint, true)
                                        .map{ p => p.asInstanceOf[MutablePoint] }

        var coreDist = calcCoreDist(startPoint, partitionIndexer)
        startPoint.coreDist = coreDist

        if (neighbors.size >= minPts) {
            processPoint(startPoint, neighbors, priorityQueue, clusterOrdering)
        }

        while (!priorityQueue.isEmpty) {
            var point = priorityQueue.dequeue()
            var neighbors = partitionIndexer.findNeighbors(point, true)
                                            .map{ p => p.asInstanceOf[MutablePoint] }
            var coreDist = calcCoreDist(point, partitionIndexer)
            point.coreDist = coreDist
            processPoint(point, neighbors, priorityQueue, clusterOrdering)
        }

    }

    private def calcCoreDist (
        origin: MutablePoint,
        partitionIndexer: PartitionIndexer ): Double = {

        var neighbors = partitionIndexer.findNeighbors(origin, false)

        var coreDist = Optics.undefinedDist

        if (neighbors.size >= minPts) {
            val sorted = neighbors.toList.sortWith(
                (p1, p2) => PartitionIndexer.distance(origin, p1) < PartitionIndexer.distance(origin, p2)
            )
            val pointOfRankMinPts = sorted(minPts-1)
            coreDist = PartitionIndexer.distance(origin, pointOfRankMinPts)
        }

        coreDist
    }

    private def processPoint (
        point: MutablePoint,
        neighbors: Iterable[MutablePoint],
        priorityQueue: PriorityQueue[MutablePoint],
        clusterOrdering: ClusterOrdering ): Unit = {

        clusterOrdering.append(point)

        update(priorityQueue, point, neighbors)

        point.processed = true

        priorityQueue.dequeue()
    }

    private def update (
        priorityQueue: PriorityQueue[MutablePoint],
        point: MutablePoint,
        neighbors: Iterable[MutablePoint] ): Unit = {

        neighbors
        .map( p => {
            var dist = math.max(point.coreDist, PartitionIndexer.distance(point, p))
            p.reachDist match {
                case Some(d) => {
                    if (p.reachDist.get > dist) {
                        p.reachDist = Option(dist)
                        updatePriorityQueue(priorityQueue, p)
                    }
                }
                case None => {
                    p.reachDist = Option(dist)
                    priorityQueue.enqueue(p)
                }
            }
        })

    }

    private def updatePriorityQueue (
        priorityQueue: PriorityQueue[MutablePoint],
        point: MutablePoint ): Unit = {

        priorityQueue.foreach(
            x => {
                if (x.pointId == point.pointId) {
                    x.reachDist = point.reachDist
                }
                else {
                    x
                }
            }
        )
    }

    private def findOneUnprocessedPoint ( points: Iterable[MutablePoint] ): Option[MutablePoint] = {
        points.find( p => !p.processed )
    }

}

object Optics {

    private val undefinedDist = -1.0

    def train (
        data: RDD[Array[Double]],
        epsilon: Double,
        minPts: Int): OpticsModel = {
        new Optics().setEpsilon(epsilon)
            .setMinPts(minPts)
            .run(data)
    }
}
