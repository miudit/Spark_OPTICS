package org.miudit.spark.mllib.clustering

import org.apache.commons.math3.ml.distance.{DistanceMeasure, EuclideanDistance}
import scala.collection.immutable.Vector
import scala.collection.mutable.{ListBuffer, ArrayBuffer}
import scala.collection.parallel.ParIterable

class PartitionIndexer (
    val partitionBox: Box,
    val points: Iterable[MutablePoint],
    val epsilon: Double,
    val minPts: Int ) extends DistanceCalculator with Serializable {

    val boxesTree = PartitionIndexer.buildRTree(partitionBox, points, epsilon, minPts)

    def findNeighbors (point: Point, onlyUnprocessed: Boolean): Iterable[Point] = {
        val queryCircleBounds = point.coordinates.map(
            coord => new BoundsInOneDimension(coord-epsilon, coord+epsilon, true)
        )
        val mbrOfQueryCircle = new Box( queryCircleBounds )

        var children = boxesTree.children
        while (!children(0).isLeaf) {
            var temp = List[BoxTreeNodeWithPoints]()
            children.map(
                child => {
                    val newChildren = child.children
                    temp ::: newChildren.filter( node => node.box.overlapsWith(mbrOfQueryCircle) )
                }
            )
            children = temp
        }

        // 得られたリーフノードIterable[BoxTreeNodeWithPoints]の全pointsに対して距離計算して範囲内の点を返す
        var result = Iterable[Point]()
        children.map( child => result ++ child.points )
        result.filter( p => PartitionIndexer.distance(point, p) <= epsilon & (!p.processed | !onlyUnprocessed) )

        result
    }

}

object PartitionIndexer extends DistanceCalculator {

    private val maxEntries = 100
    private val minEntries = 50

    /**
    * build R-tree by Sort-Tile-Recursive(STR) algorithm
    */
    def buildRTree (boundingBox: Box, points: Iterable[MutablePoint], epsilon: Double, minPts: Int): BoxTreeNodeWithPoints = {
        val dimension = boundingBox.bounds.size
        val leafNodes = createLeafNodes(boundingBox, points)
        var nodes = leafNodes
        while (nodes.size > maxEntries) {
            val nodeGroups = recursivePackBoxes(Vector(nodes), 0, dimension)
            val newNodes = nodeGroups.map(
                group => {
                    val boxes = group.map(b => {b.box})
                    val node = new BoxTreeNodeWithPoints(createMBR(boxes, dimension))
                    node.children = group
                    node
                }
            ).toList
            nodes = newNodes
        }
        val root = new BoxTreeNodeWithPoints(boundingBox)
        root.children = nodes
        root
    }

    def createLeafNodes (boundingBox: Box, points: Iterable[MutablePoint]): List[BoxTreeNodeWithPoints] = {
        val dimension = boundingBox.bounds.size
        val pointsVec = Vector(points)
        val pointGroups = recursiveSplit(pointsVec, 0, dimension)
        pointGroups.map( p => { new BoxTreeNodeWithPoints(createMBR(p, dimension), p, true)} ).toList
    }

    def recursiveSplit (pointsVec: Vector[Iterable[MutablePoint]], dimIndex: Int, dimension: Int): Vector[Iterable[MutablePoint]] = {
        var temp = Vector[Iterable[MutablePoint]]()
        pointsVec.map(
            points => {
                val numOfPoints = points.size
                val numOfPages = Math.ceil(numOfPoints.toDouble / maxEntries.toDouble).toInt
                val numOfSplitAlongAxis = Math.ceil(Math.pow(numOfPages.toDouble, (1.0/(dimension-dimIndex).toDouble))).toInt
                val sorted = points.toList.sortWith(
                    (p1, p2) => p1.coordinates(dimIndex) < p2.coordinates(dimIndex)
                )
                val sliced = sorted.sliding(numOfSplitAlongAxis, numOfSplitAlongAxis).toVector
                temp ++= sliced
            }
        )
        if (dimIndex+1 != dimension)
            recursiveSplit(temp, dimIndex+1, dimension)
        else
            temp
    }

    def recursivePackBoxes (nodesVec: Vector[List[BoxTreeNodeWithPoints]], dimIndex: Int, dimension: Int): Vector[List[BoxTreeNodeWithPoints]] = {
        var temp = Vector[List[BoxTreeNodeWithPoints]]()
        nodesVec.map(
            nodes => {
                val numOfNodes = nodes.size
                val numOfPages = Math.ceil(numOfNodes.toDouble / maxEntries.toDouble).toInt
                val numOfSplitAlongAxis = Math.ceil(Math.pow(numOfPages.toDouble, (1.0/(dimension-dimIndex).toDouble))).toInt
                val sorted = nodes.toList.sortWith(
                    (n1, n2) => n1.box.centerPoint.coordinates(dimIndex) < n2.box.centerPoint.coordinates(dimIndex)
                )
                val sliced = sorted.sliding(numOfSplitAlongAxis, numOfSplitAlongAxis).toVector
                temp ++= sliced
            }
        )
        if ( dimIndex+1 != dimension )
            recursivePackBoxes(temp, dimIndex+1, dimension)
        else
            temp
    }

    def createMBR (points: Iterable[MutablePoint], dimension: Int): Box = {
        val minPoint = new Point (Array.fill (dimension)(Double.MaxValue))
        val maxPoint = new Point (Array.fill (dimension)(Double.MinValue))
        def fold (data: Iterable[MutablePoint], zeroValue: Point, mapFunction: ((Double, Double)) => Double) = {
            data.fold(zeroValue) {
                (pt1, pt2) => {
                    new Point (pt1.coordinates.zip (pt2.coordinates).map ( mapFunction ).toArray)
                }
            }
        }
        val mins = fold (points, minPoint, x => Math.min (x._1, x._2))
        val maxs = fold (points, maxPoint, x => Math.max (x._1, x._2))

        val bounds = mins.coordinates.zip (maxs.coordinates).map ( x => new BoundsInOneDimension (x._1, x._2, true) )
        new Box(bounds)
    }

    def createMBR (boxes: List[Box], dimension: Int): Box = {
        var bounds = Array[BoundsInOneDimension]()
        for (i <- 0 to dimension-1) {
            var min = Double.MaxValue
            var max = Double.MinValue
            boxes.map(
                box => {
                    if (box.bounds(i).lower < min)
                        min = box.bounds(i).lower
                    if (box.bounds(i).upper > max)
                        max = box.bounds(i).upper
                }
            )
            bounds :+= new BoundsInOneDimension(min, max, true)
        }
        new Box(bounds)
    }

    def distance ( p1: Point, p2: Point ): Double = {
        new EuclideanDistance().compute(p1.coordinates.toArray, p2.coordinates.toArray)
    }

}

trait DistanceCalculator {

}
