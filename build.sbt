name := "iceberg-experiment"

organization := "net.heartsavior.spark"

version := "0.1"

scalaVersion := "2.11.12"

javacOptions ++= Seq(
  "-Xlint:deprecation",
  "-Xlint:unchecked",
  "-source", "1.8",
  "-target", "1.8",
  "-g:vars"
)

val sparkVersion = "2.4.0.7.2.0.0-237"
val icebergVersion = "0.9.1" // "0.9.1.7.2.0.0-237"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.iceberg" % "iceberg-spark-runtime" % icebergVersion % "provided",
  "org.scalatest" %% "scalatest" % "3.0.3" % Test withSources(),
  "junit" % "junit" % "4.12" % Test
)

resolvers += "hwx-public" at "https://nexus-private.hortonworks.com/nexus/content/groups/public/"

useCoursier := false
checksums in update := Nil
