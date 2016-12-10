package org.miudit.spark.mllib.clustering

import scala.collection.mutable.{ArrayBuffer, SynchronizedBuffer}
import org.apache.spark.rdd.RDD

class Box (
    val bounds: Array[BoundsInOneDimension],
    val boxId: Int = 0,
    val partitionId: Int = -1,
    var adjacentBoxes: List[Box] = Nil ) extends Serializable with Ordered[Box] {

    val centerPoint = calculateCenter (bounds)

    var mergeId = 1

    def contains (point: Point) = {
        bounds.zip (point.coordinates).forall( x => x._1.contains(x._2) )
    }

    def setPartitionId (partitionId: Int): Box = {
        new Box (this.bounds, this.boxId, partitionId, this.adjacentBoxes)
    }

    def expand (epsilon: Double): Box = {
        val newBounds = bounds.map( b => b.extend(epsilon) )
        val newBox = new Box(newBounds, this.boxId, this.partitionId, this.adjacentBoxes)
        newBox.mergeId = this.mergeId
        newBox
    }

    def splitAlongDimension (boxIdGenerator: BoxIdGenerator): Iterable[Box] = {
        val (longestDimension, idx) = findLongestDimensionAndItsIndex()
        val beforeLongest = if (idx > 0) bounds.take (idx) else Array[BoundsInOneDimension] ()
        val afterLongest = if (idx < bounds.size-1) bounds.drop(idx+1) else Array[BoundsInOneDimension] ()
        val splits = longestDimension.split(2)
        splits.zipWithIndex.map {
            s => {
                val newBounds = (beforeLongest :+ s._1) ++: afterLongest
                val newMergeId: Int = String.valueOf(mergeId).length() * mergeId + s._2
                val newBox = new Box (newBounds, boxIdGenerator.getNextId())
                newBox.mergeId = newMergeId
                newBox
            }
        }
        /*splits.zipWithIndex.map {
            s => {
                val newBounds = (beforeLongest :+ s._1) ++: afterLongest
                val newPartitionId = String.valueOf(partitionId).length() * partitionId + s._2
                new Box (newBounds, boxIdGenerator.getNextId(), newPartitionId)
            }
        }*/
    }

    def overlapsWith (box: Box): Boolean = {
        val bounds1 = bounds
        val bounds2 = box.bounds
        var overlap = true
        val a = bounds1.zip(bounds2).map(
            b => {
                overlap = (b._1.upper > b._2.lower && b._1.lower < b._2.upper) & overlap
            }
        )
        overlap
    }

    /*def combineBoxes (box: Box): Box = {
        val newBounds = bounds.zip(box.bounds).map(
            b => {
                val a = b
            }
        )

        new Box()
    }*/

    def overlapPoints(points: Iterable[MutablePoint]): Iterable[MutablePoint] = {
        points.filter( p => contains(p) )
    }

    private def findLongestDimensionAndItsIndex(): (BoundsInOneDimension, Int) = {
        var idx: Int = 0
        var foundBound: BoundsInOneDimension = null
        var maxLen: Double = Double.MinValue

        for (i <- 0 until bounds.size) {
            val b = bounds(i)
            val len = b.length
            if (len > maxLen) {
                foundBound = b
                idx = i
                maxLen = len
            }
        }

        (foundBound, idx)
    }

    private def calculateCenter (b: Array[BoundsInOneDimension]): Point = {
        val centerCoordinates = b.map ( x => x.lower + (x.upper - x.lower) / 2 )
        new Point (centerCoordinates)
    }

    override def compare(that: Box): Int = {
        assert (this.bounds.size == that.bounds.size)

        centerPoint.compareTo(that.centerPoint)
    }

}

class BoundsInOneDimension (
    val lower: Double,
    val upper: Double,
    val includeHigherBound: Boolean = false ) extends Serializable {

    def contains(n: Double) = {
        (n >= lower) &&  (n < upper)
    }

    def length: Double = upper - lower

    def split (n: Int): List [BoundsInOneDimension] = {
        var result: List[BoundsInOneDimension] = Nil
        val increment = (upper - lower) / n
        var currentLowerBound = lower
        for (i <- 1 to n) {
            val include = if (i < n) false else this.includeHigherBound
            val newUpperBound = currentLowerBound + increment
            val newSplit = new BoundsInOneDimension(currentLowerBound, newUpperBound, include)
            result = newSplit :: result
            currentLowerBound = newUpperBound
        }
        result.reverse
    }

    def extend (epsilon: Double): BoundsInOneDimension = {
        new BoundsInOneDimension (this.lower - epsilon, this.upper + epsilon, this.includeHigherBound)
    }

    override def equals (that: Any): Boolean = {
        if (that.isInstanceOf[BoundsInOneDimension]) {
            val typedThat = that.asInstanceOf[BoundsInOneDimension]

            typedThat.canEqual(this) &&
            this.lower == typedThat.lower &&
            this.upper == typedThat.upper &&
            this.includeHigherBound == typedThat.includeHigherBound
        }
        else {
            false
        }
    }

    def canEqual(other: Any) = other.isInstanceOf[BoundsInOneDimension]

}

class BoxCalculator (val data: RDD[Point]) {

    val numOfDimensions: Int = getNumOfDimensions(data)

    def generateBoxes (epsilon: Double, minPts: Int): (Iterable[Box], Box) = {

        val bounds = calculateBounds(data, numOfDimensions)
        val rootBox = new Box(bounds.toArray, 0)
        rootBox.mergeId = 1
        val boxTree = BoxCalculator.generateTree(rootBox, 0)

        val boxes = boxTree.flattenBoxes

        (BoxPartitioner.assignPartitionIdsToBoxes(boxes), rootBox)
        //(boxes, rootBox)

    }

    private def getNumOfDimensions (data: RDD[Point]): Int = {
        val pt = data.first()
        pt.coordinates.length
    }

    private def calculateBounds (data: RDD[Point], dimensions: Int): List[BoundsInOneDimension] = {
        val minPoint = new Point (Array.fill (dimensions)(Double.MaxValue))
        val maxPoint = new Point (Array.fill (dimensions)(Double.MinValue))
        def fold (data: RDD[Point], zeroValue: Point, mapFunction: ((Double, Double)) => Double) = {
            data.fold(zeroValue) {
                (pt1, pt2) => {
                    new Point (pt1.coordinates.zip (pt2.coordinates).map ( mapFunction ).toArray)
                }
            }
        }
        val mins = fold (data, minPoint, x => Math.min (x._1, x._2))
        val maxs = fold (data, maxPoint, x => Math.max (x._1, x._2))

        mins.coordinates.zip (maxs.coordinates).map ( x => new BoundsInOneDimension (x._1, x._2, true) ).toList
    }

}

private object BoxCalculator {

    val maxTreeLevel = 3 // log(partitionNum)

    def generateTree (root: Box, treeLevel: Int): BoxTreeNode = {

        BoxCalculator.generateTree(root, treeLevel, new BoxIdGenerator(root.boxId))

    }

    def generateTree (
        root: Box,
        treeLevel: Int,
        boxIdGenerator: BoxIdGenerator ): BoxTreeNode = {

        var result = new BoxTreeNode(root)

        result.children = if (treeLevel < maxTreeLevel) {
            val newTreeLevel = treeLevel + 1
            root.splitAlongDimension(boxIdGenerator)
                .map(x => generateTree(x, newTreeLevel, boxIdGenerator))
                .toList
        }
        else {
            List[BoxTreeNode]()
        }
        result
    }

}

abstract class BoxTreeNodeBase [T <: BoxTreeNodeBase[_]] (val box: Box) extends Serializable {
  var children: List[T] = Nil
  var level = 0

  def flatten [X <: BoxTreeNodeBase[_]]: Iterable[X] = {
    this.asInstanceOf[X] :: children.flatMap ( x => x.flatten[X] ).toList
  }

  def flattenBoxes: Iterable[Box] = {
    flatten [BoxTreeNodeBase[T]].map { x => x.box }
  }

  def flattenBoxes (predicate: T => Boolean): Iterable [Box] = {
    val result = ArrayBuffer[Box] ()
    flattenBoxes(predicate, result)
    result
  }

  private def flattenBoxes [X <: BoxTreeNodeBase[_]] (predicate: X => Boolean, buffer: ArrayBuffer[Box]): Unit = {
    if (!children.isEmpty && children.exists ( x => predicate (x.asInstanceOf[X]))) {
      children.foreach ( x => x.flattenBoxes[X](predicate, buffer))
    }
    else {
      buffer += this.box
    }
  }
}

class BoxTreeNode (box: Box) extends BoxTreeNodeBase[BoxTreeNode] (box) with Serializable {

    var numOfPoints: Long = 0

}

class BoxTreeNodeWithPoints (
    box: Box,
    //val points: SynchronizedArrayBuffer[Point] = new SynchronizedArrayBuffer[Point]()
    val points: Iterable[Point] = Nil,
    val isLeaf: Boolean = false
    ) extends BoxTreeNodeBase[BoxTreeNodeWithPoints](box) {

}

private class BoxIdGenerator (val initialId: Int) {
  var nextId = initialId

  def getNextId (): Int = {
    nextId += 1
    nextId
  }
}

class SynchronizedArrayBuffer[T] extends ArrayBuffer[T] with SynchronizedBuffer[T] {

}
