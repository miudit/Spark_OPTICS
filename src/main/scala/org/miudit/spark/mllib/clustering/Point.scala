package org.miudit.spark.mllib.clustering

import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.commons.math3.ml.distance.DistanceMeasure

class Point (
    val coordinates: Array[Double],
    val pointId: Long = 0,
    val boxId: Int = 0,
    val clusterId: Int = 0,
    var coreDist: Double = Double.MaxValue,
    var reachDist: Option[Double] = None,
    var processed: Boolean = false,
    var noise: Boolean = false ) extends Serializable with Ordered[Point] {

    var isAffected = false

    def this (pt: Point) = this (pt.coordinates, pt.pointId, pt.boxId, pt.clusterId,
        pt.coreDist, pt.reachDist, pt.processed, pt.noise)

    override def compare(that: Point): Int = {
        var result = 0
        var i = 0

        while (result == 0 && i < coordinates.size) {
            result = this.coordinates(i).compareTo(that.coordinates(i))
            i += 1
        }

        result
    }
}

class PointSortKey (point: Point) extends Ordered[PointSortKey] with Serializable {
    val boxId = point.boxId
    val pointId = point.pointId

    override def compare(that: PointSortKey): Int = {

        if (this.boxId > that.boxId) {
            1
        }
        else if (this.boxId < that.boxId) {
            -1
        }
        else if (this.pointId > that.pointId) {
            1
        }
        else if (this.pointId < that.pointId) {
            -1
        }
        else {
            0
        }
    }
}

class PointIndexer (val numOfPartitions: Int, val currentPartition: Int) {

    val multiplier = computeMultiplier (numOfPartitions)
    println("MULTIPLIER = %s".format(multiplier))
    var currentIndex: Long = 0

    def getNextIndex: Long = {
        currentIndex += 1
        currentIndex * multiplier + currentPartition
        //currentIndex
    }

    def computeMultiplier (numOfPartitions: Int) = {
        val numOfDigits = Math.floor (java.lang.Math.log10 (numOfPartitions)) + 1
        Math.round (Math.pow (10, numOfDigits))
    }

}

object PointIndexer {
    def addMetadataToPoints (
            data: RDD[Point],
            boxes: Broadcast[Iterable[Box]],
            dimensions: Broadcast[Int],
            distanceMeasure: DistanceMeasure ): RDD[(PointSortKey, Point)] = {

        val numPartitions = data.partitions.length
        val origin = new Point (Array.fill (dimensions.value)(0.0))

        data.mapPartitionsWithIndex( (partitionIndex, points) => {
            val pointIndexer = new PointIndexer (numPartitions, partitionIndex)
            points.map (pt => {
                val pointIndex = pointIndexer.getNextIndex
                val box = boxes.value.find( _.contains(pt) )
                val distanceFromOrigin = distanceMeasure.compute(pt.coordinates.toArray, origin.coordinates.toArray)
                val boxId = box match {
                    case existingBox: Some[Box] => existingBox.get.boxId
                    case _ => {
                        println("Oh NO POINT = (%s, %s)".format(pt.coordinates(0), pt.coordinates(1)))
                        0 // throw an exception?
                    }
                }
                val newPoint = new Point (pt.coordinates, pointIndex, boxId, pt.clusterId, pt.coreDist,
                    pt.reachDist, pt.processed, pt.noise)

                (new PointSortKey (newPoint), newPoint)
            })
        })

        /*data.map(
            p => {

            }
        )*/

    }
}

class MutablePoint (p: Point, val tempPointId: Long) extends Point (p) {

    var tempClusterId: Int = p.clusterId

    def toImmutablePoint: Point = new Point (this.coordinates, this.pointId, this.boxId, this.clusterId,
        this.coreDist, this.reachDist, this.processed, this.noise)

}
