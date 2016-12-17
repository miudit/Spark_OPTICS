package org.miudit.spark.mllib.clustering

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object OpticsDriver {
    def main(args: Array[String]) {
        assert( args.size == 3, "wrong arguments %s".format(args.size) )
        val inputFile = args(0)
        val epsilon = args(1).toDouble
        val minPts = args(2).toInt
        val conf = new SparkConf().setAppName("OPTICS")
        val sc = new SparkContext(conf)
        val inputCSV = sc.textFile(inputFile, 2).cache()

        val inputData: RDD[Array[Double]] = inputCSV.map(
            line => {
                line.split(",").zipWithIndex.filter( x => x._2 != 4 ).map( x => x._1.toDouble )
            }
        )

        val opticsResult = Optics.train(inputData, epsilon, minPts)

        println("finished!")
        println("input data: %s".format(inputData))
        println("result: %s".format(opticsResult.points))
    }
}
