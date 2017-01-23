package org.miudit.spark.mllib.clustering

import org.apache.spark.AccumulableParam

/***
    AccumlableType is (Int, (ClusterOrdering, Box))
***/

object COAccululableParam extends AccumulableParam[Seq[AccumlableType], AccumlableType] {

    def zero(initialValue: Seq[AccumlableType]): Seq[AccumlableType] = Seq.empty

    def addInPlace(r1: Seq[AccumlableType], r2: Seq[AccumlableType]): Seq[AccumlableType] = r1 ++ r2

    def addAccumulator(r: Seq[AccumlableType], t: AccumlableType): Seq[AccumlableType] = r :+ t

}
