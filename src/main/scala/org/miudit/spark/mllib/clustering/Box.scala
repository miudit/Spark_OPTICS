package org.miudit.spark.mllib.clustering

import scala.collection.mutable.{ArrayBuffer, SynchronizedBuffer}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Queue

class Box (
    var bounds: Array[BoundsInOneDimension],
    val boxId: Int = 0,
    val partitionId: Int = -1,
    var adjacentBoxes: List[Box] = Nil ) extends Serializable with Ordered[Box] {

    val centerPoint = calculateCenter (bounds)

    var mergeId = 1

    def volume (): Double = {
        bounds.map( bound => bound.upper - bound.lower ).reduce( (a,b) => a*b )
    }

    def contains (point: Point) = {
        bounds.zip (point.coordinates).forall( x => x._1.contains(x._2) )
    }

    def setPartitionId (partitionId: Int): Box = {
        var newBox = new Box (this.bounds, this.boxId, partitionId, this.adjacentBoxes)
        newBox.mergeId = this.mergeId
        newBox
    }

    def setBoxId (boxId: Int): Box = {
        var newBox = new Box (this.bounds, boxId, this.partitionId, this.adjacentBoxes)
        newBox.mergeId = this.mergeId
        newBox
    }

    def expand (epsilon: Double): Box = {
        val newBounds = bounds.map( b => b.extend(epsilon) )
        val newBox = new Box(newBounds, this.boxId, this.partitionId, this.adjacentBoxes)
        newBox.mergeId = this.mergeId
        newBox
    }

    def splitAlongDimension (points: Iterable[Point], boxIdGenerator: BoxIdGenerator): Iterable[(Box, Iterable[Point])] = {
        val (longestDimension, idx) = findLongestDimensionAndItsIndex()
        val beforeLongest = if (idx > 0) bounds.take (idx) else Array[BoundsInOneDimension] ()
        val afterLongest = if (idx < bounds.size-1) bounds.drop(idx+1) else Array[BoundsInOneDimension] ()
        val median = findMedian(idx, points)
        val splits = longestDimension.splitWhere(median)
        splits.zipWithIndex.map {
            s => {
                val newBounds = (beforeLongest :+ s._1) ++: afterLongest
                val newMergeId: Int = 10 * mergeId + s._2
                val newBox = new Box (newBounds, boxIdGenerator.getNextId())
                newBox.mergeId = newMergeId
                (newBox, newBox.overlapPoints(points.map(x => new MutablePoint(x, 0))))
            }
        }
    }

    def findMedian (index: Int, points: Iterable[Point]): Double = {
        val result = points.map( p => p.coordinates(index) ).toList
        result
        .sortWith(_ < _)
        .drop(result.length/2).head
    }

    def overlapsWith (box: Box): Boolean = {
        val bounds1 = bounds
        val bounds2 = box.bounds
        var overlap = true
        val a = bounds1.zip(bounds2).map(
            b => {
                overlap = (b._1.upper > b._2.lower && b._1.lower < b._2.upper) && overlap
            }
        )
        overlap
    }

    def overlapPoints(points: Iterable[MutablePoint]): Iterable[MutablePoint] = {
        points.filter( p => contains(p) )
    }

    def overlappingRegion ( box: Box ): Box = {
        val bounds = this.bounds.zip(box.bounds).map( boundPair => {
            new BoundsInOneDimension( Math.max( boundPair._1.lower, boundPair._2.lower ), Math.min( boundPair._1.upper, boundPair._2.upper ) )
        } )
        new Box( bounds )
    }

    def addBox ( box: Box ): Unit = {
        val newBounds = this.bounds.zip(box.bounds).map(boundPair => new BoundsInOneDimension(Math.min(boundPair._1.lower, boundPair._2.lower), Math.max(boundPair._1.upper, boundPair._2.upper)))
        bounds = newBounds
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
        (n >= lower) &&  ((n < upper) || (includeHigherBound && (n <= upper)))
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

    def splitWhere (value: Double): List[BoundsInOneDimension] = {
        val newBounds1 = new BoundsInOneDimension(lower, value, false)
        val newBounds2 = new BoundsInOneDimension(value, upper, includeHigherBound)
        List(newBounds1, newBounds2)
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

    def generateBoxes (epsilon: Double, minPts: Int): (Iterable[Box], Box, Iterable[Box]) = {

        val bounds = calculateBounds(data, numOfDimensions)
        val rootBox = new Box(bounds.toArray, 0)
        rootBox.mergeId = 1
        //MEMO: if data is larger than memory of one node, then possibly can't split by median (due to passing points)
        val boxTree = BoxCalculator.generateTree(rootBox, data.collect.toIterable, 0)

        // reassign box Ids
        val boxes = boxTree.flattenBoxes(x => true)
            .zipWithIndex.map( x => x._1.setBoxId(x._2) )

        val allBoxes = boxTree.flattenBoxes

        (BoxPartitioner.assignPartitionIdsToBoxes(boxes), rootBox, allBoxes)
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

class SimpleBoxCalculator (val data: List[MutablePoint]) {

    val numOfDimensions: Int = getNumOfDimensions(data)

    private def getNumOfDimensions (data: List[MutablePoint]): Int = {
        val pt = data(0)
        pt.coordinates.length
    }

    def calculateBounds (): List[BoundsInOneDimension] = {
        val minPoint = new Point (Array.fill (numOfDimensions)(Double.MaxValue))
        val maxPoint = new Point (Array.fill (numOfDimensions)(Double.MinValue))
        def fold (data: List[MutablePoint], zeroValue: Point, mapFunction: ((Double, Double)) => Double) = {
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

    var maxTreeLevel = Math.round(Math.pow(Optics.numOfExecterNodes, 1.0/2)).toInt
    if ( Optics.numOfExecterNodes == 1 ) {
        maxTreeLevel = 0
    }

    def generateTree (root: Box, points: Iterable[Point], treeLevel: Int): BoxTreeNode = {

        BoxCalculator.generateTree(root, points, treeLevel, new BoxIdGenerator(root.boxId))

    }

    def generateTree (
        root: Box,
        points: Iterable[Point],
        treeLevel: Int,
        boxIdGenerator: BoxIdGenerator ): BoxTreeNode = {

        var result = new BoxTreeNode(root)

        result.children = if (treeLevel < maxTreeLevel) {
            val newTreeLevel = treeLevel + 1
            root.splitAlongDimension(points, boxIdGenerator)
                .map(x => generateTree(x._1, x._2, newTreeLevel, boxIdGenerator))
                .toList
        }
        else {
            List[BoxTreeNode]()
        }
        result
    }

}

abstract class BoxTreeNodeBase [T <: BoxTreeNodeBase[_]] (var box: Box) extends Serializable {
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

case class BoxTreeNodeWithPoints (
    _box: Box,
    val points: Iterable[Point] = Nil,
    val isLeaf: Boolean = false
) extends BoxTreeNodeBase[BoxTreeNodeWithPoints](_box) {

    val insertionQueue = new Queue[BoxTreeNodeWithPoints]
    val localInsertionQueue = new Queue[BoxTreeNodeWithPoints]

    var temporary = false

    def resetProcessedFlags (): Unit = {
        children.foreach(_.points.foreach(_.processed = false))
        if ( isLeaf )
            None
        else
            children.foreach(_.resetProcessedFlags())
    }

    def setTemporary (): Unit = {
        temporary = true
    }

    def applyLocalInsertionQueue (): Unit = {
        while ( this.localInsertionQueue.size > 0 ) {
            val entry = this.localInsertionQueue.dequeue()
            this.children :+= entry
            this.box.addBox(entry.box)
        }
        this.children.foreach( child => {
            child.applyLocalInsertionQueue()
        } )
    }

    def getDescendantInsertionQueueSize (): Int = {
        this.children match {
            case Nil => this.insertionQueue.size
            case _ => this.insertionQueue.size + this.children.map(_.getDescendantInsertionQueueSize()).reduce(_+_)
        }
    }

    def getDescendantLocalInsertionQueueSize (): Int = {
        this.children match {
            case Nil => this.localInsertionQueue.size
            case _ => this.localInsertionQueue.size + this.children.map(_.getDescendantLocalInsertionQueueSize()).reduce(_+_)
        }
    }

    def entryNum (): Int = {
        if ( this.children.size == 0 ) {
            0
        }
        else {
            this.children.size + this.children.map( child => child.entryNum() ).reduce( (a,b) => a+b )
        }
    }

    def pointNum (): Int = {
        if (this.points.size != 0) {
            this.points.size
        }
        else {
            this.children.map( child => child.pointNum() ).reduce( (a,b) => a+b )
        }
    }

    def TreeInsertion (): List[BoxTreeNodeWithPoints] = {
        if ( this.isLeaf ) {
            None
        }
        else {
            while ( !insertionQueue.isEmpty ) {
                val entry = insertionQueue.dequeue()
                //If e refers to a single element, then insert e into suitable child of current node
                // skip
                // If e refers to a subtree
                if ( entry.getLevel() < this.getLevel() && areaCriterion(entry) ) {
                    // Insert e into the insertion queue of the suitable child of current node
                    this.children.minBy(_.enlargement(entry)).insertionQueue.enqueue(entry)
                }
                else if ( entry.getLevel() == this.getLevel() && overlapCriterion(entry) ) {
                    //Insert e into the local insertion queue of current node
                    this.localInsertionQueue.enqueue(entry)
                    //this.children :+= entry
                    //this.box.addBox(entry.box)
                }
                else {
                    // Insert all the entries of e/subtree into the insertion queue of current node
                    entry.children.foreach( child => {
                        this.insertionQueue.enqueue(child)
                    })
                }
            }
            /*insertionQueue.foreach( e => {
                //If e refers to a single element, then insert e into suitable child of current node
                // skip
                // If e refers to a subtree
                if ( e.getLevel() < this.getLevel() && areaCriterion(e) ) {
                    // Insert e into the insertion queue of the suitable child of current node
                    this.children.minBy(_.enlargement(e)).insertionQueue.enqueue(e)
                }
                else if ( e.getLevel() == this.getLevel() && overlapCriterion(e) ) {
                    //Insert e into the local insertion queue of current node
                    this.localInsertionQueue.enqueue(e)
                }
                else {
                    //Insert all the entries of e/subtree into the insertion queue of current node
                    e.children.foreach(this.insertionQueue.enqueue(_))
                }
            } )*/
            checkInsertionQueue()
        }
        multipleSplit()
    }

    def checkInsertionQueue (): Unit = {
        children.foreach( child => {
            if ( !child.insertionQueue.isEmpty ){
                child.TreeInsertion()
            }
        } )
    }

    def areaCriterion ( subtree: BoxTreeNodeWithPoints ): Boolean = {
        //println("areaCriterion is called")
        val suitableChild = this.children.minBy(_.enlargement(subtree))
        val thisEnlargement = suitableChild.enlargement(this)
        val totalEnlargement = subtree.children match {
            case Nil => subtree.points.map( entry => this.children.minBy(_.enlargement(entry)) ).zip(subtree.points).map(x => x._1.enlargement(x._2)).reduce((a,b) => a+b)
            case _ => subtree.children.map( entry => this.children.minBy(_.enlargement(entry)) ).zip(subtree.children).map(node => node._1.enlargement(node._2)).reduce((a,b) => a+b)
        }
        //val totalEnlargement = subtree.children.map( entry => this.children.minBy(_.enlargement(entry)) ).zip(subtree.children).map(node => node._1.enlargement(node._2)).reduce((a,b) => a+b)
        thisEnlargement <= totalEnlargement
    }

    def overlapCriterion ( subtree: BoxTreeNodeWithPoints ): Boolean = {
        /* subtree を this に挿入した場合の，オーバーラップ領域の拡大するサイズ */
        if ( !this.children.isEmpty ) {
            val copiedThis1 = this.copyNode()
            copiedThis1.children :+= subtree
            val overlapEnlargement = copiedThis1.totalOverlappingVolume()
            val copiedThis2 = this.copyNode()
            subtree.children.foreach( child => {
                copiedThis2.children.minBy(_.enlargement(child)).children :+= child
                // apply enlargement for the parent node of children
                copiedThis2.children.minBy(_.enlargement(child)).box.addBox(child.box)
            } )
            val childrenOverlapEnlargement = copiedThis1.totalOverlappingVolume()
            overlapEnlargement <= childrenOverlapEnlargement
        }
        else {
            true
        }
    }

    def copyNode (): BoxTreeNodeWithPoints = {
        val newNode = this.copy()
        newNode.children = this.children.map(x => x.copyNode())
        newNode
    }

    /* returns area enlargement to include 'that' */
    def enlargement ( that: BoxTreeNodeWithPoints ): Double = {
        val thisVolume = this.box.bounds.map(_.length).reduce((a,b) => a*b)
        //val enlargedVolume = this.box.bounds.zip(that.box.bounds).map(a => Math.min(a._1.lower, a._2.lower) - Math.max(a._1.upper, a._2.upper)).reduce((a,b) => a*b)
        val enlargedVolume = this.box.bounds.zip(that.box.bounds).map(a => Math.max(a._1.upper, a._2.upper) - Math.min(a._1.lower, a._2.lower)).reduce((a,b) => a*b)
        val enlargement = enlargedVolume - thisVolume
        assert( enlargement >= 0, "something wrong" )
        enlargement
    }

    def enlargement ( that: Point ): Double = {
        val thisVolume = this.box.bounds.map(_.length).reduce((a,b) => a*b)
        //val enlargedVolume = this.box.bounds.zip(that.coordinates).map(a => Math.min(a._1.lower, a._2.lower) - Math.max(a._1.upper, a._2.upper)).reduce((a,b) => a*b)
        val enlargedVolume = this.box.bounds.zip(that.coordinates).map(a => Math.max(a._1.upper, a._2) - Math.min(a._1.lower, a._2)).reduce((a,b) => a*b)
        val enlargement = enlargedVolume - thisVolume
        assert( enlargement >= 0, "something wrong" )
        enlargement
    }

    /* returns the volume of overlapping region */
    def totalOverlappingVolume (): Double = {
        /* calculate pairwise overlapping region */
        val overlappingVolumes = this.children.combinations(2).filter(comb => comb(0).box.overlapsWith(comb(1).box)).map(comb => comb(0).overlappingVolume(comb(1)))
        //overlappingRegionの体積出す→さらにoverlappingRegionを出して引く?
        if ( overlappingVolumes.isEmpty ) {
            0.0
        }
        else {
            overlappingVolumes.reduce((a,b) => a*b)
        }
    }

    def overlappingVolume ( node: BoxTreeNodeWithPoints ): Double = {
        this.box.bounds.zip(node.box.bounds).map( boundPair => {
            if ( (boundPair._1.upper <= boundPair._2.lower || boundPair._1.lower >= boundPair._2.upper) ) {
                0
            }
            else {
                Math.min( boundPair._1.upper, boundPair._2.upper ) - Math.max( boundPair._1.lower, boundPair._2.lower )
            }
        } )
        .reduce( (a,b) => a*b )
    }

    def multipleSplit (): List[BoxTreeNodeWithPoints] = {
        /* Execute the multiple split for C.
            If new sibling nodes have been created because of splits:
            Insert suitable entries for them into the local insertion queue of the parent of C */
        List(this)
    }

    def getLevel (): Int = {
        var currentNode = this
        var result = 0
        while( !currentNode.isLeaf ) {
            currentNode = currentNode.children(0)
            result = result + 1
        }
        result
    }

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
