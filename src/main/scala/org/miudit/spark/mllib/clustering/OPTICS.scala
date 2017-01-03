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

        println("RUN !!!")

        //println("partitionedData = %s".format(partitionedData))
        //println("num of partitions of partitionedData = %s".format(partitionedData.partitions.size))

        //partitionedData.foreachPartition(partition => println("Size of partition = %s".format(partition.size)))

        /*partitionedData.foreachPartition(
            partition => {
                val points = partition.toList.map(x => x._2)
                val mostright = points.maxBy(_.coordinates(0))
                val mostleft = points.minBy(_.coordinates(0))
                println("RIGHT = (%s, %s), LEFT = (%s, %s)".format(mostright.coordinates(0), mostright.coordinates(1),
                    mostleft.coordinates(0), mostleft.coordinates(1) ))
                //println("PARTITIONED MOST LEFT POINT = (%s, %s)".format(mostleft.coordinates(0), mostleft.coordinates(1)))
            }
        )*/

        /*partitionedData.boxes.foreach(
            box => {
                val bounds = box.bounds
                println("PBOX = x:(%s, %s), y:(%s, %s)".format(
                    bounds(0).lower, bounds(0).upper, bounds(1).lower, bounds(1).upper
                ))
            }
        )*/

        /*partitionedData.mapPartitionsWithIndex( (idx, it) => {
            it.map(x => println("pointId = %s".format(x._2.pointId)))
        }.toIterator ).collect*/

        val broadcastBoxes = data.sparkContext.broadcast(partitionedData.boxes)

        println("START PARTIAL CLUSTERING")
        val partialClusters = partitionedData.mapPartitionsWithIndex (
            (partitionIndex, it) => {
                val boxes = broadcastBoxes.value
                //println("partitionIndex = %s".format(partitionIndex))
                //println("partitionSize = %s".format(it.size))
                //println("partitionIterator = %s".format(it))
                val partitionBoundingBox = boxes.find(  _.partitionId == partitionIndex ).get
                //println("partitionBoundingBox = %s".format(partitionBoundingBox))
                val partialResult = partialClustering(it, partitionBoundingBox)
                //println("partialResult = %s".format(partialResult))
                Vector( (partitionBoundingBox.mergeId, (partialResult, partitionBoundingBox)) ).toIterator
            },
            preservesPartitioning = true
        )

        //partialClusters.collect.foreach(x => println("aaa = %s".format(x)))

        //println("COUNT = %s".format(partialClusters.count()))

        //partialClusters.foreachPartition(x => None)
        //val newPartialClusters = partialClusters.mapPartitions(x => x, true).persist()

        /*partialClusters.mapPartitionsWithIndex( (idx, it) => {
            it.map(x => println("mergeId = %s".format(x._1)))
        }.toIterator ).collect*/

        partialClusters.cache()

        //partitionedData.take(100).foreach(x => println(x._2.coreDist))
        //partialClusters.take(100).foreach(x => x._2._1.map(p => println(p.coreDist)))
        //println(partialClusters.toLocalIterator.size)

        //partialClusters.mapPartitionsWithIndex( (idx, it) => it.toList.map(x => println("mergeId = %s".format(x._1))).toIterator ).collect

        //partialClusters.foreachPartition( partition => println("Size of result partition = %s".format(partition.size)) )
        //broadcastBoxes.destroy()
        //partialClusters.toLocalIterator.foreach( x => println("mergeId = %s".format(x._1)) )

        val partitionIdsToMergeIds = partitionedData.boxes.map ( x => (x.partitionId, x.mergeId) ).toMap
        //println("partitionIdsToMergeIds = %s".format(partitionIdsToMergeIds))

        println("START MERGING")
        //val mergedClusters = mergeClusters(partialClusters, partitionedData.boxes, partitionIdsToMergeIds)
        var mergedClusters = mergeClusters(partialClusters, partitionedData.boxes, partitionIdsToMergeIds)

        assert(mergedClusters.partitions.size == 1, "Merged Clusters RDD Partition Size != 1")

        println("START EXTRACTING")
        val extractedResult = mergedClusters.map (
            co => {
                val result = Optics.extractClusterOrdering(co, epsilon)
                println("RESULT CLUSTER SIZE = %s".format(result._2))
                result._1
            }
        )

        //println("num of partitions of mergedClusters = %s".format(mergedClusters.partitions.size))

        /*mergedClusters.foreach( co => {
            co.map( x => println("CLUSTER ID = %s, POINT = (%s, %s), coreDist = %s, reachDist = %s".format(x.clusterId, x.coordinates(0), x.coordinates(1), x.coreDist, x.reachDist)) )
        } )*/

        println("RUN FINISHED")

        new OpticsModel(extractedResult, epsilon, minPts)
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

        var clusterId = 1

        var tempPoint: Option[MutablePoint] = None
        /*while ( { tempPoint = findOneUnprocessedPoint(points.values); tempPoint.isDefined } ) {
            var point = tempPoint.get
            //println("call expand !!!")
            println("foundPointId = %s".format(point.pointId))
            expand(point, points, partitionIndexer, priorityQueue, clusterOrdering)
            println("expand finished! point's is processed = %s".format(point.processed))
            //println("i am here")
        }*/
        points.values.foreach(
            p => {
                if ( !p.processed ) {
                    //println("next expand pointId = %s".format(p.pointId))
                    expand(p, points, partitionIndexer, priorityQueue, clusterOrdering)
                    //println("expand finished! point's is processed = %s".format(p.processed))
                }
            }
        )

        //println("partialClustering finished!")

        /*points.values
        .filter( p => p.noise )
        .map(
            p => {
                // set all points' processed as false for merging
                //p.processed = false
                clusterOrdering.append(p)
            }
        )*/

        //println("NOISE SIZE = %s".format(points.values.filter(p => p.noise).size))
        //println("UNPROCESSED SIZE = %s".format(points.values.filter(p => !p.processed).size))
        //println("PARTIAL COUNT = %s, DISTINCE PARTIAL COUNT = %s".format(clusterOrdering.size, clusterOrdering.distinct.size))

        println("Partial Clustering Finished ")
        clusterOrdering
    }

    private def expand (
        startPoint: MutablePoint,
        points: Map[Long, MutablePoint],
        partitionIndexer: PartitionIndexer,
        priorityQueue: PriorityQueue[MutablePoint],
        clusterOrdering: ClusterOrdering ): Unit = {

        var neighbors = partitionIndexer.findNeighbors(startPoint, false)
                                        .map{ p => p.asInstanceOf[MutablePoint] }

        startPoint.processed = true

        //println("neighborssize = %s".format(neighbors.size))

        var coreDist = calcCoreDist(startPoint, partitionIndexer)
        startPoint.coreDist = coreDist

        /*if (clusterOrdering.find(p => p.pointId == startPoint.pointId).isDefined) {
            println("ALREADY EXISTS A")
        }*/

        clusterOrdering.append(startPoint)

        /*if (neighbors.size >= minPts) {
            processPoint(startPoint, neighbors, priorityQueue, clusterOrdering)
            println("processed in expand = %s".format(startPoint.processed))
        }

        while (!priorityQueue.isEmpty) {
            assert( priorityQueue.size > 0, "priorityQueue has no element @ expand" )
            var point = priorityQueue.dequeue()
            var neighbors = partitionIndexer.findNeighbors(point, true)
                                            .map{ p => p.asInstanceOf[MutablePoint] }
            var coreDist = calcCoreDist(point, partitionIndexer)
            point.coreDist = coreDist
            processPoint(point, neighbors, priorityQueue, clusterOrdering)
        }*/

        if (startPoint.coreDist != Optics.undefinedDist) {
            update(priorityQueue, startPoint, neighbors)
            while (!priorityQueue.isEmpty) {
                var nextPoint = priorityQueue.dequeue()
                var nextNeighbors = partitionIndexer.findNeighbors(nextPoint, false)
                                                    .map{ p => p.asInstanceOf[MutablePoint] }
                nextPoint.processed = true
                nextPoint.coreDist = calcCoreDist(nextPoint, partitionIndexer)
                /*if (clusterOrdering.find(p => p.pointId == nextPoint.pointId).isDefined) {
                    println("ALREADY EXISTS B POINT = (%s, %s)".format(nextPoint.coordinates(0), nextPoint.coordinates(1)))
                }*/
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

        //println("ORIGIN = (%s, %s), NEIGHBOR SIZE = %s".format(origin.coordinates(0), origin.coordinates(1), neighbors.size))

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

        /*if (clusterOrdering.find(p => p.pointId == point.pointId).isDefined) {
            println("ALREADY EXISTS C")
        }*/
        clusterOrdering.append(point)

        update(priorityQueue, point, neighbors)

        point.processed = true

        assert( priorityQueue.size > 0, "priorityQueue has no element @ processPoint" )
        priorityQueue.dequeue()
        //println("dequeued")
    }

    private def update (
        priorityQueue: PriorityQueue[MutablePoint],
        point: MutablePoint,
        neighbors: Iterable[MutablePoint],
        debug: Boolean = false ): Unit = {

        neighbors
        .filter( p => !p.processed )
        .map( p => {
            var dist = math.max(point.coreDist, PartitionIndexer.distance(point, p))
            p.reachDist match {
                case Some(d) => {
                    // for process affected point at merging phase
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
        partialClusters: RDD[(Int, (ClusterOrdering, Box))],
        boxes: Iterable[Box],
        partitionIdsToMergeIds: Map[Int, Int] ): RDD[ClusterOrdering] = {

        /*partialClusters.treeAggregate()(
            seqOp: (org.apache.spark.rdd.RDD.U, org.miudit.spark.mllib.clustering.MutablePoint) => org.apache.spark.rdd.RDD.U,
            combOp: (org.apache.spark.rdd.RDD.U, org.apache.spark.rdd.RDD.U) => org.apache.spark.rdd.RDD.U,
            BoxCalculator.maxTreeLevel
        )*/

        //var partitionIdsToMergeIds: Map[Int, Int] = partitionIdsToMergeIds

        //partialClusters.foreachPartition(x => println("SIZE = %s".format(x.size)))

        val tp1 = new Point(Array(10.0, 10.0))
        val tp2 = new Point(Array(20.0, 10.0))
        val tp3 = new Point(Array(10.2, 10.4))
        val tmp1 = new MutablePoint(tp1, 0)
        val tmp2 = new MutablePoint(tp2, 1)
        val tmp3 = new MutablePoint(tp3, 2)
        val tplist = Array(tmp1, tmp2, tmp3)
        val tbounds = Array(new BoundsInOneDimension(9.0, 11.0), new BoundsInOneDimension(9.0, 11.0))
        val tbox = new Box(tbounds)
        val overlapping = tbox.overlapPoints(tplist)
        //println("OVERLAPPING DETECTION")
        //println("P1 = (%s, %s), P2 = (%s, %s)".format(tmp1.coordinates(0), tmp1.coordinates(1), tmp2.coordinates(0), tmp2.coordinates(1)))
        //println("BOUNDS = (%s, %s), (%s, %s)".format(tbounds(0).lower, tbounds(0).upper, tbounds(1).lower, tbounds(1).upper ))
        //println("OVERLAP SIZE = %s".format(overlapping.size))
        //println("OVERLAP SIZE = %s".format(overlapping.toList(0).coordinates(0)))
        //overlapping.toList.map(p => "POINT (%s, %s) IS OVERLAPPING !".format(p.coordinates(0), p.coordinates(1)))

        var partialClusterOrderings = partialClusters.cache()

        while (partialClusterOrderings.getNumPartitions > 1) {
            println("MERGE STEP : REMAINING PARTITION NUM = %s".format(partialClusterOrderings.getNumPartitions))
            partialClusterOrderings = partialClusterOrderings.mapPartitionsWithIndex(
                (index, iterator) => {
                    val tempList = iterator.toList

                    assert( tempList.size == 1, "Bad Partitioning ? tempList size = %s".format(tempList.size) )

                    val temp = tempList.map(
                        p => {
                            (p._1/10, p._2)
                        }
                    ).toIterator
                    temp
                }
            )
            .reduceByKey(
                (p1, p2) => {
                    /*println("BOX 1 = (%s, %s), (%s, %s), BOX 2 = (%s, %s), (%s, %s)".format(
                        p1._2.bounds(0).lower, p1._2.bounds(0).upper, p1._2.bounds(1).lower, p1._2.bounds(1).upper,
                        p2._2.bounds(0).lower, p2._2.bounds(0).upper, p2._2.bounds(1).lower, p2._2.bounds(1).upper
                    ))*/
                    println("MERGE !")
                    val mergeResult = merge(p1._1, p2._1, p1._2, p2._2)
                    //println("MERGE RESULT SIZE = %s".format(mergeResult.size))
                    val newBox = boxes.find( _.mergeId == p1._2.mergeId ).get
                    //println("NEW BOX = %s".format(newBox))
                    ( mergeResult, newBox )
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

        /*println("+++++++++++++++++++")
        println("EXPANDED BOX1 = (%s, %s), (%s, %s)".format(
            expandedBox1.bounds(0).lower, expandedBox1.bounds(0).upper, expandedBox1.bounds(1).lower, expandedBox1.bounds(1).upper
        ))
        println("+++++++++++++++++++")*/

        //println("HERE A")

        var tempId = 0
        // 全点のprocessedをfalseにリセット
        val points1 = co1.toIterable.map( p => { p.processed = false; p} )
        val points2 = co2.toIterable.map( p => { p.processed = false; p} )

        val mostright = points1.maxBy(_.coordinates(0))
        val mostleft = points1.minBy(_.coordinates(0))
        //println("MOST RIGHT POINT = (%s, %s)".format(mostright.coordinates(0), mostright.coordinates(1)))
        //println("MOST LEFT POINT = (%s, %s)".format(mostleft.coordinates(0), mostleft.coordinates(1)))

        val mostright2 = points2.maxBy(_.coordinates(0))
        val mostleft2 = points2.minBy(_.coordinates(0))
        //println("MOST RIGHT POINT = (%s, %s)".format(mostright2.coordinates(0), mostright2.coordinates(1)))
        //println("MOST LEFT POINT = (%s, %s)".format(mostleft2.coordinates(0), mostleft2.coordinates(1)))

        //println("HERE B")

        val expandedPoints1 = points1 ++ expandedBox1.overlapPoints(points2)
        val expandedPoints2 = points2 ++ expandedBox2.overlapPoints(points1)

        //println("before1 size = %s, after1 size = %s".format(points1.size, expandedPoints1.size))
        //println("before2 size = %s, after2 size = %s".format(points2.size, expandedPoints2.size))

        //println("HERE C")

        val indexer1 = new PartitionIndexer(expandedBox1, expandedPoints1, epsilon, minPts)
        val indexer2 = new PartitionIndexer(expandedBox2, expandedPoints2, epsilon, minPts)

        //println("HERE D")

        markAffectedPoints(indexer1.boxesTree, indexer2.boxesTree)

        //println("HERE E")

        // affected pointとしてマークした情報を反映
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

        //println("MARKED POINTS1 SIZE = %s, DISTINCT MARKED POINTS1 SIZE = %s".format(markedPoints1.size, markedPoints1.toList.distinct.size))
        //println("MARKED POINTS2 SIZE = %s, DISTINCT MARKED POINTS2 SIZE = %s".format(markedPoints2.size, markedPoints2.toList.distinct.size))

        //println("HERE F")

        var newClusterOrdering = new ClusterOrdering()

        /*println("AFFECTED SIZE 1 = %s, AFFECTED SIZE 2 = %s, UNAFFECTED SIZE 1 = %s, UNAFFECTED SIZE 2 = %s".format(
            markedCO1.filter(p => p.isAffected).size,
            markedCO2.filter(p => p.isAffected).size,
            markedCO1.filter(p => !p.isAffected).size,
            markedCO2.filter(p => !p.isAffected).size
        ))*/

        // affectes size = 0 の場合は単純にco1,co2を結合
        if ( markedCO1.filter(p => p.isAffected).size == 0 ) {
            //println("CO1 ONLY UNAFFECTED")
            newClusterOrdering = newClusterOrdering ++ markedCO1
        }
        else {
            processClusterOrdering(markedPoints1, markedPoints2, indexer1, markedCO1, newClusterOrdering)
        }
        if ( markedCO2.filter(p => p.isAffected).size == 0 ) {
            //println("CO2 ONLY UNAFFECTED")
            newClusterOrdering = newClusterOrdering ++ markedCO2
        }
        else {
            processClusterOrdering(markedPoints2, markedPoints1, indexer2, markedCO2, newClusterOrdering)
        }

        /*println("AFFECTED SIZE = %s, UNAFFECTED SIZE = %s".format(
            newClusterOrdering.filter(p => p.isAffected).size,
            newClusterOrdering.filter(p => !p.isAffected).size
        ))*/

        //if ( markedCO1.filter(p => p.isAffected).size > 0 )
        //assert( markedCO1.filter(p => p.isAffected).size > 0, "NO AFFECTED POINT IN CO1" )
        //processClusterOrdering(markedPoints1, markedPoints2, indexer1, markedCO1, newClusterOrdering)
        //else
        //    None
        //println("SSSSSSSS")
        //if ( markedCO2.filter(p => p.isAffected).size > 0 )
        //assert( markedCO2.filter(p => p.isAffected).size > 0, "NO AFFECTED POINT IN CO2" )
        //processClusterOrdering(markedPoints2, markedPoints1, indexer2, markedCO2, newClusterOrdering)
        //else None

        //println("HERE G")

        //println("CO1 SIZE = %s, CO2 SIZE = %s".format(co1.size, co2.size))
        //println("NEW CLUSTER ORDERING SIZE = %s".format(newClusterOrdering.size))

        newClusterOrdering
    }

    private def processClusterOrdering (
        points1: Iterable[MutablePoint],
        points2: Iterable[MutablePoint],
        indexer: PartitionIndexer,
        clusterOrdering: ClusterOrdering,
        newClusterOrdering: ClusterOrdering): Unit = {

        var priorityQueue = new PriorityQueue[MutablePoint]()(Ordering.by[MutablePoint, Double](_.reachDist.get).reverse)

        //val debug = clusterOrdering.find(_.pointId == 50).isDefined

        //println("PROCESS CLUSTER ORDERING !!!")

        //while (clusterOrdering.size > 0) {
        //while ( clusterOrdering.filter(p => !p.processed).size > 0 ) {
        while ( clusterOrdering.filter(p => p.isAffected && !p.processed).size > 0 ) {
            //println("NOT PROCESSED SIZE = %s".format(clusterOrdering.filter(p => !p.processed).size))
            //println("TTTTTTTTTTTTTTTTTTTTT")
            if (!priorityQueue.isEmpty) {
                assert( priorityQueue.size > 0, "priorityQueue has no element @ processClusterOrdering" )
                val q = priorityQueue.dequeue()
                if (q.isAffected){
                    //println("AFFECTED")
                }
                process(points1, points2, q, indexer, priorityQueue, newClusterOrdering)
            }
            else {
                //println("PQ IS EMPTY")
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
                /*if( clusterOrdering.filter(p => p.isAffected && !p.processed).size == 0 ){
                    println("NO MORE AFFECTED OR UNPROCESSED POINTS")
                }*/
            }
        }

        //println("START FOR ALL NON AFFECTED")
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

    private def processAffectedPoint (
        points1: Iterable[MutablePoint],
        points2: Iterable[MutablePoint],
        point: MutablePoint,
        indexer: PartitionIndexer,
        priorityQueue: PriorityQueue[MutablePoint],
        newClusterOrdering: ClusterOrdering ): Unit = {

        val debug = points1.find(_.pointId == 50).isDefined

        val neighbors = indexer.findNeighbors(point, false)
            .map{ p => p.asInstanceOf[MutablePoint] }

        if (neighbors.size >= minPts) {
            //point.noise = false
            //println("BEFORE COREDIST = %s".format(point.coreDist))
            point.coreDist = calcCoreDist(point, indexer)
            //println("AFTER COREDIST = %s".format(point.coreDist))
            update(priorityQueue, point, neighbors, debug)
            //point.processed = true
            newClusterOrdering.append(point)
            //println("APPENDED POINT = %s".format(point))
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

        // predecessorがUNDEFINEDの時targetsがどうなるか
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

        val debug = points1.find(_.pointId == 50).isDefined

        if (point.isAffected) {
            processAffectedPoint(points1, points2, point, indexer, priorityQueue, newClusterOrdering)
        }
        else {
            //println("PROCESS NON AFFECTED POINT !!!")
            processNonAffectedPoint(points1, points2, point, indexer, priorityQueue, newClusterOrdering)
        }

    }

    private def markAffectedPoints (
        root1: BoxTreeNodeWithPoints,
        root2: BoxTreeNodeWithPoints ): Unit = {

        //while ( ! (root1.isLeaf && root2.isLeaf ) ) {
        if ( ! (root1.isLeaf && root2.isLeaf ) ) {
            if ( root1.box.overlapsWith(root2.box) ) {
                for (x <- root1.children; y <- root2.children) {
                    markAffectedPoints(x, y)
                }
            }
        }
        else {
            /*println("----------------------------")
            println("BOUNDS1 = (%s, %s), (%s, %s)".format(root1.box.bounds(0).lower, root1.box.bounds(0).upper,
                root1.box.bounds(1).lower, root1.box.bounds(1).upper ))
            println("BOUNDS2 = (%s, %s), (%s, %s)".format(root2.box.bounds(0).lower, root2.box.bounds(0).upper,
                root2.box.bounds(1).lower, root2.box.bounds(1).upper ))
            println("----------------------------")*/
            if ( root1.box.overlapsWith(root2.box) ) {
                //println("OVERLAPPING !!")
                //root1.points.foreach(p => println("P1 = (%s, %s)".format(p.coordinates(0), p.coordinates(1))))
                //root2.points.foreach(p => println("P2 = (%s, %s)".format(p.coordinates(0), p.coordinates(1))))
                for (x <- root1.points; y <- root2.points) {
                    //println("x: %s, y: %s".format(x, y))
                    //println("DIST = %s".format(PartitionIndexer.distance(x, y)))
                    if (PartitionIndexer.distance(x, y) < epsilon) {
                        //println("MARK AFFECTED !!!!!!")
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

        println("finished!")
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
                                //println("ID CHANGED")
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
