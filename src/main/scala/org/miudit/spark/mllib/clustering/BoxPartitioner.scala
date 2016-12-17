package org.miudit.spark.mllib.clustering

import org.apache.spark.Partitioner

class BoxPartitioner (val boxes: Iterable[Box]) extends Partitioner {

    assert (boxes.forall(_.partitionId >= 0))

    boxes.foreach( b => println("partition id = %s".format(b.partitionId)) )
    boxes.foreach( b => println("box id = %s".format(b.boxId)) )

    private val boxIdsToPartitions = boxes.map ( x => (x.boxId, x.partitionId) ).toMap

    override def numPartitions: Int = boxes.size

    override def getPartition(key: Any): Int = {
        key match {
            case k: PointSortKey => boxIdsToPartitions(k.boxId)
            case boxId: Int => boxIdsToPartitions(boxId)
            case pt: Point => boxIdsToPartitions(pt.boxId)
            case _ => {
                println("here")
                0
            } // throw an exception?
        }
    }

}

private object BoxPartitioner {

    def assignPartitionIdsToBoxes (boxes: Iterable[Box]): Iterable[Box] = {
        boxes.zip (0 until boxes.size).map ( x => x._1.setPartitionId(x._2) )
    }

}
