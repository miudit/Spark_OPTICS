package org.miudit.spark.mllib.clustering

import scala.collection.mutable.{ArrayBuffer, PriorityQueue}
import scala.util.control.Breaks.{break, breakable}
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

        var partialClusters = partitionedData.mapPartitionsWithIndex (
            (partitionIndex, it) => {
                val boxes = broadcastBoxes.value
                val partitionBoundingBox = boxes.find(  _.partitionId == partitionIndex ).get
                Vector( (partitionBoundingBox.mergeId, (partialClustering(it, partitionBoundingBox), partitionBoundingBox)) ).toIterator
            },
            preservesPartitioning = true
        )

        val partitionIdsToMergeIds = partitionedData.boxes.map ( x => (x.partitionId, x.mergeId) ).toMap

        val mergedClusters = mergeClusters(partialClusters, partitionedData.boxes, partitionIdsToMergeIds)

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
        .map(
            p => {
                // set all points' processed as false for merging
                p.processed = false
                clusterOrdering.append(p)
            }
        )

        clusterOrdering
    }

    private def mergeClusters (
        partialClusters: RDD[(Int, (ClusterOrdering, Box))],
        boxes: Iterable[Box],
        partitionIdsToMergeIds: Map[Int, Int] ): RDD[ClusterOrdering] = {

        /*partialClusters.treeAggregate()(
            seqOp: (org.apache.spark.rdd.RDD.U, org.miudit.spark.mllib.clustering.MutablePoint) => org.apache.spark.rdd.RDD.U,
            combOp: (org.apache.spark.rdd.RDD.U, org.apache.spark.rdd.RDD.U) => org.apache.spark.rdd.RDD.U,
            BoxCalculator.maxTreeLevel
        )*/

        //var partitionIdsToMergeIds: Map[Int, Int] = partitionIdsToMergeIds

        var partialClusterOrderings = partialClusters

        while (partialClusterOrderings.getNumPartitions > 1) {
            partialClusterOrderings = partialClusterOrderings.mapPartitionsWithIndex(
                (index, iterator) => {
                    assert( iterator.size > 1, "Bad Partitioning ?" )

                    iterator.map( p => (p._1/10, p._2) )
                }
            )
            .reduceByKey(
                (p1, p2) => {
                    ( merge(p1._1, p2._1, p1._2, p2._2),
                        boxes.find( _.mergeId == p1._2.mergeId ).get )
                },
                partialClusterOrderings.getNumPartitions/2
            )
        }

        partialClusterOrderings.map( co => co._2._1 )
    }

    private def merge (
        co1: ClusterOrdering,
        co2: ClusterOrdering,
        box1: Box,
        box2: Box ): ClusterOrdering = {

        val expandedBox1 = box1.expand(epsilon)
        val expandedBox2 = box2.expand(epsilon)

        var tempId = 0
        // 全点のprocessedをfalseにリセット
        val points1 = co1.toIterable.map( p => { p.processed = false; p} )
        val points2 = co2.toIterable.map( p => { p.processed = false; p} )

        val expandedPoints1 = points1 ++ expandedBox1.overlapPoints(points2)
        val expandedPoints2 = points2 ++ expandedBox2.overlapPoints(points1)

        val indexer1 = new PartitionIndexer(expandedBox1, expandedPoints1, epsilon, minPts)
        val indexer2 = new PartitionIndexer(expandedBox2, expandedPoints2, epsilon, minPts)

        markAffectedPoints(indexer1.boxesTree, indexer2.boxesTree)

        // affected pointとしてマークした情報を反映
        val markedPoints1 = indexer1.points.zip(points1).map( p => {
            p._2.isAffected = p._1.isAffected
            p._2
        } )
        val markedPoints2= indexer2.points.zip(points2).map( p => {
            p._2.isAffected = p._1.isAffected
            p._2
        } )

        var newClusterOrdering = new ClusterOrdering()

        processClusterOrdering(markedPoints1, markedPoints2, indexer1, co1, newClusterOrdering)
        processClusterOrdering(markedPoints2, markedPoints1, indexer2, co2, newClusterOrdering)

        newClusterOrdering
    }

    private def processClusterOrdering (
        points1: Iterable[MutablePoint],
        points2: Iterable[MutablePoint],
        indexer: PartitionIndexer,
        clusterOrdering: ClusterOrdering,
        newClusterOrdering: ClusterOrdering): Unit = {

        var priorityQueue = new PriorityQueue[MutablePoint]()(Ordering.by[MutablePoint, Double](_.reachDist.get).reverse)

        while (clusterOrdering.size > 0) {
            if (!priorityQueue.isEmpty) {
                val q = priorityQueue.dequeue()
                process(points1, points2, q, indexer, priorityQueue, newClusterOrdering)
            }
            else {
                breakable(
                    while (clusterOrdering.size > 0) {
                        val x = findOneUnprocessedPoint(clusterOrdering).get
                        if (x.isAffected) {
                            processAffectedPoint(points1, points2, x, indexer, priorityQueue, newClusterOrdering)
                            if (!priorityQueue.isEmpty)
                                break
                        }
                    }
                )
            }
        }

        while (!priorityQueue.isEmpty) {
            val q = priorityQueue.dequeue()
            process(points1, points2, q, indexer, priorityQueue, newClusterOrdering)
        }

    }

    private def processAffectedPoint (
        points1: Iterable[MutablePoint],
        points2: Iterable[MutablePoint],
        point: MutablePoint,
        indexer: PartitionIndexer,
        priorityQueue: PriorityQueue[MutablePoint],
        newClusterOrdering: ClusterOrdering ): Unit = {

        val neighbors = indexer.findNeighbors(point, false)
            .map{ p => p.asInstanceOf[MutablePoint] }

        if (neighbors.size >= minPts) {
            point.noise = false
            point.coreDist = calcCoreDist(point, indexer)
            update(priorityQueue, point, neighbors)
            point.processed = true
            newClusterOrdering.append(point)
        }
    }

    private def processNonAffectedPoint (
        points1: Iterable[MutablePoint],
        points2: Iterable[MutablePoint],
        point: MutablePoint,
        indexer: PartitionIndexer,
        priorityQueue: PriorityQueue[MutablePoint],
        newClusterOrdering: ClusterOrdering ): Unit = {

        def findPrecedessor ( point: MutablePoint ): Option[MutablePoint] = {
            indexer.findNeighbors(point, false)
                .map{ p => p.asInstanceOf[MutablePoint] }
                .find( p => {
                    point.reachDist.get == Math.max( point.coreDist, PartitionIndexer.distance(point, p) )
                } )
        }

        val successors = indexer.findNeighbors(point, false)
            .map{ p => p.asInstanceOf[MutablePoint] }
            .filter( p => { point.pointId == findPrecedessor(p).get.pointId } )

        // predecessorがUNDEFINEDの時targetsがどうなるか
        val predecessor = Iterable[MutablePoint](findPrecedessor(point).get)

        val targets = successors ++ predecessor

        targets.filter( p => ! p.processed )
            .map( p => {
                if ( ! priorityQueue.exists( q => p.pointId == q.pointId ) ) {
                    val neighbors = indexer.findNeighbors(p, false)
                    if ( neighbors.size < minPts )
                        p.reachDist = None
                    else
                        p.reachDist = Option( math.max( point.coreDist, PartitionIndexer.distance(p, point) ) )
                }
                else if ( math.max( point.coreDist, PartitionIndexer.distance(p, point) ) < p.reachDist.get ) {
                    p.reachDist = Option( math.max( point.coreDist, PartitionIndexer.distance(p, point) ) )
                    updatePriorityQueue(priorityQueue, p)
                }
            } )

        point.processed = true
        newClusterOrdering.append(point)
    }

    private def process (
        points1: Iterable[MutablePoint],
        points2: Iterable[MutablePoint],
        point: MutablePoint,
        indexer: PartitionIndexer,
        priorityQueue: PriorityQueue[MutablePoint],
        newClusterOrdering: ClusterOrdering ): Unit = {

        if (point.isAffected) {
            processAffectedPoint(points1, points2, point, indexer, priorityQueue, newClusterOrdering)
        }
        else {
            processNonAffectedPoint(points1, points2, point, indexer, priorityQueue, newClusterOrdering)
        }

    }

    private def markAffectedPoints (
        root1: BoxTreeNodeWithPoints,
        root2: BoxTreeNodeWithPoints ): Unit = {

        while ( ! (root1.isLeaf && root2.isLeaf ) ) {
            if ( root1.box.overlapsWith(root2.box) ) {
                for (x <- root1.children; y <- root2.children) {
                    markAffectedPoints(x, y)
                }
            }
        }

        if ( root1.box.overlapsWith(root2.box) ) {
            for (x <- root1.points; y <- root2.points) {
                if (PartitionIndexer.distance(x, y) < epsilon) {
                    x.isAffected = true
                    y.isAffected = true
                }
            }
        }

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
