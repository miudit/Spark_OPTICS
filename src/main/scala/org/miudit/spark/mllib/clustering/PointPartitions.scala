package org.miudit.spark.mllib.clustering

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.commons.math3.ml.distance.{DistanceMeasure, EuclideanDistance}

class PointPartitions (
    val points: RDD[(PointSortKey, Point)],
    val boxes: Iterable[Box],
    val boundingBox: Box,
    val allBoxes: Iterable[Box] ) extends ShuffledRDD [PointSortKey, Point, Point] (points, new BoxPartitioner(boxes))


object PointPartitions {

    def apply (data: RDD[Point],
        epsilon: Double,
        minPts: Int ): PointPartitions = {

        val sc = data.sparkContext
        val boxCalculator = new BoxCalculator(data)
        val (boxes, boundingBox, allBoxes) = boxCalculator.generateBoxes(epsilon, minPts)
        boxes.foreach( box => {
            val bound1 = box.bounds(0)
            val bound2 = box.bounds(1)
            println("box bound = x:(%s,%s), y:(%s,%s)".format(bound1.lower, bound1.upper, bound2.lower, bound2.upper))
        })
        val broadcastBoxes = sc.broadcast(boxes)
        val broadcastNumOfDimensions = sc.broadcast(boxCalculator.numOfDimensions)

        val pointsInBoxes = PointIndexer.addMetadataToPoints(
            data,
            broadcastBoxes,
            broadcastNumOfDimensions,
            new EuclideanDistance())

        PointPartitions(pointsInBoxes, boxes, boundingBox, allBoxes)
    }

    def apply (pointsInBoxes: RDD[(PointSortKey, Point)], boxes: Iterable[Box], boundingBox: Box, allBoxes: Iterable[Box]): PointPartitions = {
        new PointPartitions(pointsInBoxes, boxes, boundingBox, allBoxes)
    }

}
