package org.miudit.spark.mllib.clustering

import scala.util.Random
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

        val indexers = partitionedData.mapPartitionsWithIndex(
            (idx, it) => {
                val boxes = broadcastBoxes.value
                val partitionBoundingBox = boxes.find(  _.partitionId == idx ).get
                var tempPointId: Long = 0
                val pts = it.map {
                    x => {
                        tempPointId += 1
                        var newPt = new MutablePoint(x._2, tempPointId)

                        newPt
                    }
                }.toList
                Vector((idx, partitionBoundingBox, pts)).toIterator
            }, preservesPartitioning = true
        ).toArray
        .map( x => new PartitionIndexer(x._2, x._3.toIterable, epsilon, minPts, x._1) )
        .toIterable

        val broadcastIndexers = data.sparkContext.broadcast(indexers)

        val partialClusters = partitionedData.mapPartitionsWithIndex (
            (partitionIndex, it) => {

                val boxes = broadcastBoxes.value
                val indexers = broadcastIndexers.value
                val partitionBoundingBox = boxes.find(  _.partitionId == partitionIndex ).get
                val partitionIndexer = indexers.find( _.partitionIndex == partitionIndex ).get
                val partialResult = partialClustering(it, partitionBoundingBox, partitionIndexer)

                Vector( (partitionBoundingBox.mergeId, (partialResult, partitionBoundingBox, partitionIndexer)) ).toIterator
            },
            preservesPartitioning = true
        )

        var mergedClusters = mergeClusters(partialClusters, partitionedData.boxes, partitionedData.allBoxes)

        assert(mergedClusters.partitions.size == 1, "Merged Clusters RDD Partition Size != 1")

        val extractedResult = mergedClusters.map (
            co => {
                Optics.extractClusterOrdering(co, epsilon)
            }
        )

        new OpticsModel(extractedResult, epsilon, minPts)
    }

    private def partialClustering (
        it: Iterator[(PointSortKey, Point)],
        boundingBox: Box,
        partitionIndexer: PartitionIndexer): ClusterOrdering = {

        var tempPointId: Long = 0
        var points = partitionIndexer.points
        var priorityQueue = new PriorityQueue[MutablePoint]()(Ordering.by[MutablePoint, Double](_.reachDist.get).reverse)
        var clusterOrdering = new ClusterOrdering

        var clusterId = 1
        var tempPoint: Option[MutablePoint] = None
        points.foreach(
            p => {
                if ( !p.processed ) {
                    expand(p, partitionIndexer, priorityQueue, clusterOrdering)
                }
            }
        )

        clusterOrdering
    }

    private def expand (
        startPoint: MutablePoint,
        partitionIndexer: PartitionIndexer,
        priorityQueue: PriorityQueue[MutablePoint],
        clusterOrdering: ClusterOrdering ): Unit = {

        var neighbors = partitionIndexer.findNeighbors(startPoint, false)
                                        .map{ p => p.asInstanceOf[MutablePoint] }

        startPoint.processed = true

        var coreDist = calcCoreDist(startPoint, partitionIndexer)
        startPoint.coreDist = coreDist

        clusterOrdering.append(startPoint)

        if (startPoint.coreDist != Optics.undefinedDist) {
            update(priorityQueue, startPoint, neighbors)
            while (!priorityQueue.isEmpty) {
                var nextPoint = priorityQueue.dequeue()
                var nextNeighbors = partitionIndexer.findNeighbors(nextPoint, false)
                                                    .map{ p => p.asInstanceOf[MutablePoint] }
                nextPoint.processed = true
                nextPoint.coreDist = calcCoreDist(nextPoint, partitionIndexer)

                clusterOrdering.append(nextPoint)
                if ( nextPoint.coreDist != Optics.undefinedDist ) {
                    update(priorityQueue, nextPoint, nextNeighbors)
                }
            }
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

        assert( priorityQueue.size > 0, "priorityQueue has no element @ processPoint" )
        priorityQueue.dequeue()
    }

    private def update (
        priorityQueue: PriorityQueue[MutablePoint],
        point: MutablePoint,
        neighbors: Iterable[MutablePoint],
        debug: Boolean = false ): Unit = {

        /*if (debug) {
            println("BEFORE PQ SIZE @ update = %s".format(priorityQueue.size))
            println("unprocessed neighbor size = %s".format(neighbors.filter(!_.processed).size))
        }*/

        neighbors
        .filter( p => !p.processed )
        .foreach( p => {
            var dist = math.max(point.coreDist, PartitionIndexer.distance(point, p))
            p.reachDist match {
                case Some(d) => {
                    // for processing affected point at merging phase
                    if( priorityQueue.find(x => x.pointId == p.pointId).isEmpty ) {
                        priorityQueue.enqueue(p)
                    }
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

        /*if (debug) {
            println("AFTER PQ SIZE @ update = %s".format(priorityQueue.size))
        }*/

    }

    private def updatePriorityQueue (
        priorityQueue: PriorityQueue[MutablePoint],
        point: MutablePoint ): Unit = {

        var updated = false

        priorityQueue.foreach(
            x => {
                if (x.pointId == point.pointId) {
                    x.reachDist = point.reachDist
                    updated = true
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

    private def mergeClusters (
        partialClusters: RDD[(Int, (ClusterOrdering, Box, PartitionIndexer))],
        boxes: Iterable[Box],
        allBoxes: Iterable[Box] ): RDD[ClusterOrdering] = {

        var partialClusterOrderings = partialClusters.cache()

        while (partialClusterOrderings.getNumPartitions > 1) {

            println("numOfPartitions = %s".format(partialClusterOrderings.getNumPartitions))

            val partialCOArray = partialClusterOrderings.toArray

            val broadcastCO = partialClusterOrderings.sparkContext.broadcast(partialCOArray)

            /* partialClustersのPartitionIndexerのリストを作成するように変更 */
            val indexers = partialCOArray.map( co => {
                co._2._3.partitionIndex = co._1
                (co._1, co._2._3)
            } )
            .combinations(2).toList
            .filter( pair => {
                pair(0)._1/10 == pair(1)._1/10
            })
            .map( pair => {
                val indexer1 = pair(0)._2
                val indexer2 = pair(1)._2
                val pointsOfIndexer1 = indexer1.points
                val pointsOfIndexer2 = indexer2.points
                val expandedBox1 = indexer1.boxesTree.box.expand(epsilon)
                val expandedBox2 = indexer2.boxesTree.box.expand(epsilon)
                val expandedPoints1 = expandedBox1.overlapPoints(pointsOfIndexer2).toList
                val expandedPoints2 = expandedBox2.overlapPoints(pointsOfIndexer1).toList
                val boxcalculator1 = new SimpleBoxCalculator(expandedPoints1)
                val boxcalculator2 = new SimpleBoxCalculator(expandedPoints2)
                val newBox1 = new Box(boxcalculator1.calculateBounds().toArray)
                val newBox2 = new Box(boxcalculator2.calculateBounds().toArray)
                val newNode1 = new BoxTreeNodeWithPoints(newBox1, expandedPoints1, true)
                newNode1.setTemporary()
                val newNode2 = new BoxTreeNodeWithPoints(newBox2, expandedPoints2, true)
                newNode2.setTemporary()
                indexer1.boxesTree.box.addBox(newBox1)
                indexer2.boxesTree.box.addBox(newBox2)
                indexer1.addTempBox(newBox1)
                indexer2.addTempBox(newBox2)
                indexer1.addTempPoints(expandedPoints1)
                indexer2.addTempPoints(expandedPoints2)
                indexer1.boxesTree.children = indexer1.boxesTree.children :+ newNode1
                indexer2.boxesTree.children = indexer2.boxesTree.children :+ newNode2
                List(indexer1, indexer2)
            }).flatten

            val broadcastIndexers = partialClusters.sparkContext.broadcast(indexers)

            partialClusterOrderings = partialClusterOrderings.mapPartitionsWithIndex(
                (index, iterator) => {
                    val tempList = iterator.toList

                    assert( tempList.size == 1, "Bad Partitioning ? tempList size = %s".format(tempList.size) )

                    val temp = tempList.map(
                        p => {
                            (p._1/10, (p._2._1, p._2._2, p._2._3))
                        }
                    ).toIterator
                    temp
                }
            )
            .reduceByKey(
                (p1, p2) => {
                    val indexer1 = broadcastIndexers.value.find( _.partitionIndex == p1._2.mergeId ).get
                    val indexer2 = broadcastIndexers.value.find( _.partitionIndex == p2._2.mergeId ).get

                    /*
                    * expand indexer1 and indexer2
                    * inserted new nodes have some flag for removing when merging indexers
                    */
                    /*val pointsOfIndexer1 = indexer1.points
                    val pointsOfIndexer2 = indexer2.points
                    val expandedBox1 = indexer1.boxesTree.box.expand(epsilon)
                    val expandedBox2 = indexer2.boxesTree.box.expand(epsilon)
                    val expandedPoints1 = expandedBox1.overlapPoints(pointsOfIndexer2).toList
                    val expandedPoints2 = expandedBox2.overlapPoints(pointsOfIndexer1).toList
                    val boxcalculator1 = new SimpleBoxCalculator(expandedPoints1)
                    val boxcalculator2 = new SimpleBoxCalculator(expandedPoints2)
                    val newBox1 = new Box(boxcalculator1.calculateBounds().toArray)
                    val newBox2 = new Box(boxcalculator2.calculateBounds().toArray)
                    val newNode1 = new BoxTreeNodeWithPoints(newBox1, expandedPoints1, true)
                    newNode1.setTemporary()
                    val newNode2 = new BoxTreeNodeWithPoints(newBox2, expandedPoints2, true)
                    newNode2.setTemporary()
                    indexer1.boxesTree.box.addBox(newBox1)
                    indexer2.boxesTree.box.addBox(newBox2)
                    indexer1.addTempBox(newBox1)
                    indexer2.addTempBox(newBox2)
                    indexer1.addTempPoints(expandedPoints1)
                    indexer2.addTempPoints(expandedPoints2)
                    indexer1.boxesTree.children = indexer1.boxesTree.children :+ newNode1
                    indexer2.boxesTree.children = indexer2.boxesTree.children :+ newNode2*/

                    //val mergeResult = merge(p1._1, p2._1, indexer1, indexer2)
                    val mergeResult = p1._1 ++ p2._1
		    
	            mergeResult.foreach(_.isAffected = false)
                    val newBox = allBoxes.find( _.mergeId == p1._2.mergeId/10 ).get

                    indexer1.removeTempNode()
                    indexer2.removeTempNode()

                    //val mergedIndexer = indexer1.mergeIndexers(indexer2)
                    val mergedIndexer = indexer1.simpleMerge(indexer2)

                    ( mergeResult, newBox, mergedIndexer )
                },
                partialClusterOrderings.getNumPartitions/2
            ).cache()
        }

        partialClusterOrderings.map( co => co._2._1 )
    }

    private def merge (
        co1: ClusterOrdering,
        co2: ClusterOrdering,
        indexer1: PartitionIndexer,
        indexer2: PartitionIndexer ): ClusterOrdering = {

        val expandedBox1 = indexer1.partitionBox
        val expandedBox2 = indexer2.partitionBox

        var tempId = 0

        val points1 = co1.toIterable.map( p => { p.processed = false; p} )
        val points2 = co2.toIterable.map( p => { p.processed = false; p} )

        val expandedPoints1 = points1 ++ expandedBox1.overlapPoints(points2)
        val expandedPoints2 = points2 ++ expandedBox2.overlapPoints(points1)

        markAffectedPoints(indexer1.boxesTree, indexer2.boxesTree)

        println("MARKING FINISHED affected size 1 = %s / %s".format(indexer1.points.filter(_.isAffected).size, indexer1.points.size))
        println("MARKING FINISHED affected size 2 = %s / %s".format(indexer2.points.filter(_.isAffected).size, indexer2.points.size))

        // reflect affected points
        val markedPoints1 = points1.map( p => {
            val pointInIndexer1 = indexer1.points.find( x => x.pointId == p.pointId )
            assert( pointInIndexer1.isDefined, "Something wrong" )
            p.isAffected = pointInIndexer1.get.isAffected
            p
        })
        val markedPoints2 = points2.map( p => {
            val pointInIndexer2 = indexer2.points.find( x => x.pointId == p.pointId )
            assert( pointInIndexer2.isDefined, "Something wrong" )
            p.isAffected = pointInIndexer2.get.isAffected
            p
        })

        //affectedをco1, co2にも反映
        val markedCO1 = co1.map( p => {
            val pointInIndexer1 = indexer1.points.find( x => x.pointId == p.pointId )
            assert( pointInIndexer1.isDefined, "Something wrong2" )
            p.isAffected = pointInIndexer1.get.isAffected
            p
        })
        val markedCO2 = co2.map( p => {
            val pointInIndexer2 = indexer2.points.find( x => x.pointId == p.pointId )
            assert( pointInIndexer2.isDefined, "Something wrong2" )
            p.isAffected = pointInIndexer2.get.isAffected
            p
        })

        println("REFLECTION FINISHED")

        indexer1.resetProcessedFlags()
        indexer2.resetProcessedFlags()

        var newClusterOrdering = new ClusterOrdering()

        // simple join CO1 if there are no affected point
        if ( markedCO1.filter(p => p.isAffected).size == 0 ) {
            newClusterOrdering = newClusterOrdering ++ markedCO1
        }
        else {
            processClusterOrdering(markedPoints1, markedPoints2, indexer1, markedCO1, newClusterOrdering)
            println("EXIT ProcessClusterOrdering2")
            //newClusterOrdering = newClusterOrdering ++ markedCO1
        }
        if ( markedCO2.filter(p => p.isAffected).size == 0 ) {
            newClusterOrdering = newClusterOrdering ++ markedCO2
        }
        else {
            processClusterOrdering(markedPoints2, markedPoints1, indexer2, markedCO2, newClusterOrdering)
            //newClusterOrdering = newClusterOrdering ++ markedCO2
        }

        println("JOIN FINISHED")

        newClusterOrdering
    }

    private def processClusterOrdering (
        points1: Iterable[MutablePoint],
        points2: Iterable[MutablePoint],
        indexer: PartitionIndexer,
        clusterOrdering: ClusterOrdering,
        newClusterOrdering: ClusterOrdering): Unit = {

        var priorityQueue = new PriorityQueue[MutablePoint]()(Ordering.by[MutablePoint, Double](_.reachDist.get).reverse)

        while ( clusterOrdering.filter(p => p.isAffected && !p.processed).size > 0 ) {

            if (!priorityQueue.isEmpty) {
                assert( priorityQueue.size > 0, "priorityQueue has no element @ processClusterOrdering" )
                val q = priorityQueue.dequeue()
                process(points1, points2, q, indexer, priorityQueue, newClusterOrdering)
            }
            else {
                // PQ is Empty
                breakable(
                    for (i <- 0 to clusterOrdering.size-1) {
                        val p = clusterOrdering(i)
                        if ( !p.processed ){
                            if (p.isAffected) {
                                processAffectedPoint(points1, points2, p, indexer, priorityQueue, newClusterOrdering)
                                if (!priorityQueue.isEmpty){
                                    break
                                }
                            }
                        }
                    }
                )
            }
        }

        // append not added unaffected points
        while (!priorityQueue.isEmpty) {
            assert( priorityQueue.size > 0, "priorityQueue has no element @ processClusterOrdering 2" )
            val q = priorityQueue.dequeue()
            process(points1, points2, q, indexer, priorityQueue, newClusterOrdering)
        }

        assert( clusterOrdering.filter(p => p.isAffected && !p.processed).size == 0, "AFFECTED POINTS REMAINING" )

        // process for all unprocessed unaffected points if exists
        breakable(
            for ( i <- 0 to clusterOrdering.size ) {
                val p = clusterOrdering(i)
                if ( !p.isAffected && !p.processed ) {
                    processNonAffectedPoint(points1, points2, p, indexer, priorityQueue, newClusterOrdering)
                }
                if ( clusterOrdering.filter(p => !p.isAffected && !p.processed).size == 0 )
                    break
            }
        )

    }

    private def processClusterOrdering2 (
        points1: Iterable[MutablePoint],
        points2: Iterable[MutablePoint],
        indexer: PartitionIndexer,
        clusterOrdering: ClusterOrdering,
        newClusterOrdering: ClusterOrdering): Unit = {

        var priorityQueue = new PriorityQueue[MutablePoint]()(Ordering.by[MutablePoint, Double](_.reachDist.get).reverse)

        while ( clusterOrdering.filter(p => !p.processed).size > 0 ) {
            //println("BEFORE = %s".format(clusterOrdering.filter(!_.processed).size))
            if (!priorityQueue.isEmpty) {
                assert( priorityQueue.size > 0, "priorityQueue has no element @ processClusterOrdering" )
                val q = priorityQueue.dequeue()
                //println("BEFORE P = %s".format(clusterOrdering.filter(!_.processed).size))
                process(points1, points2, q, indexer, priorityQueue, newClusterOrdering)
                //println("AFTER P = %s".format(clusterOrdering.filter(!_.processed).size))
                //println("PROCESSED = %S".format(clusterOrdering.filter(p => p.pointId==q.pointId)(0).processed))
            }
            else {
                // PQ is Empty
                breakable(
                    for (i <- 0 to clusterOrdering.size-1) {
                        val p = clusterOrdering(i)
                        if ( !p.processed ){
                            if (p.isAffected) {
                                //println("BEFORE PA = %s".format(clusterOrdering.filter(!_.processed).size))
                                processAffectedPoint(points1, points2, p, indexer, priorityQueue, newClusterOrdering)
                                //println("AFFECTED POINT PROCESSED = %s".format(p.processed))
                                //println("AFTER PA = %s".format(clusterOrdering.filter(!_.processed).size))
                                //println("PROCESS AFFECTED POINT")
                                if (!priorityQueue.isEmpty){
                                    println("BREAK")
                                    break
                                }
                            }
                        }
                    }
                )
            }
            //println("AFTER = %s".format(clusterOrdering.filter(!_.processed).size))
            println("UNPROCESSED SIZE = %s".format(clusterOrdering.filter(p => !p.processed).size))
            //println("PPPPPP")
        }

        while (!priorityQueue.isEmpty) {
            assert( priorityQueue.size > 0, "priorityQueue has no element @ processClusterOrdering 2" )
            val q = priorityQueue.dequeue()
            process(points1, points2, q, indexer, priorityQueue, newClusterOrdering)
            println("PROCESSED = %S".format(clusterOrdering.filter(p => p.pointId==q.pointId)(0).processed))
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
            point.coreDist = calcCoreDist(point, indexer)
            update(priorityQueue, point, neighbors, true)
            newClusterOrdering.append(point)
        }

        point.processed = true
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
                    if ( point.reachDist.isDefined )
                        point.reachDist.get == Math.max( point.coreDist, PartitionIndexer.distance(point, p) )
                    else
                        false
                } )
        }

        val predecessor = findPrecedessor(point)
        var predecessorSingletonList = Iterable[MutablePoint]()
        if (predecessor.isDefined) {
            predecessorSingletonList = Iterable[MutablePoint](predecessor.get)
        }

        val successors = indexer.findNeighbors(point, false)
            .map{ p => p.asInstanceOf[MutablePoint] }
            .filter( p => {
                val predecessor = findPrecedessor(p)
                if (predecessor.isDefined)
                    point.pointId == findPrecedessor(p).get.pointId
                else
                    false
            } )

        val targets = successors ++ predecessorSingletonList

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

        //if ( ! (root1.isLeaf && root2.isLeaf ) ) {
        if ( !root1.isLeaf && !root2.isLeaf ) {
            //println("AAA")
            if ( root1.box.overlapsWith(root2.box) ) {
                for (x <- root1.children; y <- root2.children) {
                    markAffectedPoints(x, y)
                }
            }
        }
        else if ( !root1.isLeaf && root2.isLeaf ) {
            //println("BBB")
            if ( root1.box.overlapsWith(root2.box) ) {
                for (x <- root1.children) {
                    markAffectedPoints(x, root2)
                }
            }
        }
        else if ( root1.isLeaf && !root2.isLeaf ) {
            //println("CCC")
            if ( root1.box.overlapsWith(root2.box) ) {
                for (x <- root2.children) {
                    markAffectedPoints(root1, x)
                }
            }
        }
        else if ( root1.isLeaf && root2.isLeaf ) {

            if ( root1.box.overlapsWith(root2.box) ) {
                for (x <- root1.points; y <- root2.points) {
                    if (PartitionIndexer.distance(x, y) < epsilon) {
                        x.isAffected = true
                        y.isAffected = true
                    }
                }
            }
        }
    }

}

object Optics {

    private val undefinedDist = -1.0

    private val noiseId = -1

    var numOfExecterNodes = 2
    var maxEntriesForRTree = 0

    def train (
        data: RDD[Array[Double]],
        epsilon: Double,
        minPts: Int): OpticsModel = {

        val startTime = System.nanoTime()
        val result = new Optics().setEpsilon(epsilon)
            .setMinPts(minPts)
            .run(data)

        val endTime = System.nanoTime()

        println("ELAPSED TIME = %s ms".format( (endTime - startTime) / 1000000.0 ))

        result
    }

    def extractClusterOrdering (
        clusterOrdering: ClusterOrdering,
        epsilon: Double ): (ClusterOrdering, Int) = {

        var clusterId = noiseId


        ( clusterOrdering.map(
            p => {
                p.reachDist match {
                    case Some(d) => {
                        if ( p.reachDist.get > epsilon ) {
                            if ( p.coreDist != -1 && p.coreDist <= epsilon ) {
                                clusterId = clusterId + 1
                                p.clusterId = clusterId
                            }
                            else {
                                p.clusterId = noiseId
                            }
                        }
                        else {
                            p.clusterId = clusterId
                        }
                    }
                    case None => {
                        if ( p.coreDist != -1 && p.coreDist <= epsilon ) {
                            clusterId = clusterId + 1
                            p.clusterId = clusterId
                        }
                        else {
                            p.clusterId = noiseId
                        }
                    }
                }
                p
            }
        ),
        clusterId )
    }

}
