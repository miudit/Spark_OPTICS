package org.miudit.spark.mllib.clustering

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object OpticsDriver {
    def main(args: Array[String]) {
        assert( args.size == 5, "wrong arguments %s".format(args.size) )
        val inputFile = args(0)
        val epsilon = args(1).toDouble
        val minPts = args(2).toInt
        Optics.numOfExecterNodes = args(3).toInt
        Optics.maxEntriesForRTree = args(4).toInt
        val conf = new SparkConf().setAppName("OPTICS")
        val sc = new SparkContext(conf)
        sc.setLogLevel("INFO")
        val inputCSV = sc.textFile(inputFile, 2)//.cache()

        val inputData: RDD[Array[Double]] = inputCSV.map(
            line => {
                line.split(",").zipWithIndex.filter( x => x._2 != 4 ).map( x => x._1.toDouble )
            }
        )

        val opticsResult = Optics.train(inputData, epsilon, minPts)

        opticsResult.points.collect.foreach(
            co => co.map( x => println("CLUSTER ID = %s, POINT = (%s, %s), coreDist = %s, reachDist = %s".format(x.clusterId, x.coordinates(0), x.coordinates(1), x.coreDist, x.reachDist)) )
            //co => None
        )
    }
}
