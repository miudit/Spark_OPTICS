name := "spark_optics"

organization := "org.miudit"

version := "0.0.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1" % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.6.1" % "provided"
)
