// Package Information

name := "example" // change to project name
organization := "com.thoughtworks" // change to your org
version := "0.1-SNAPSHOT"
scalaVersion := "2.11.8"

// Spark Information
val sparkVersion = "2.3.2"

assemblyJarName in assembly := s"de-training-${version.value}.jar"

test in assembly := {}

// allows us to include spark packages
resolvers += "bintray-spark-packages" at
  "https://dl.bintray.com/spark-packages/maven/"

resolvers += "Typesafe Simple Repository" at
  "http://repo.typesafe.com/typesafe/simple/maven-releases/"

resolvers += "MavenRepository" at
  "https://mvnrepository.com/"

libraryDependencies ++= Seq(
  // spark core
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",

  // spark-modules
  "org.apache.spark" %% "spark-graphx" % sparkVersion % "provided",
  // "org.apache.spark" %% "spark-mllib" % sparkVersion,

  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",

  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,

  // spark packages
  "graphframes" % "graphframes" % "0.5.0-spark2.1-s_2.11",

  // testing
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.scalacheck" %% "scalacheck" % "1.12.2" % "test",

  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.4.1"
)



//mainClass in(Compile, packageBin) := Some("com.thoughtworks.example.MainClass")

// Compiler settings. Use scalac -X for other options and their description.
// See Here for more info http://www.scala-lang.org/files/archive/nightly/docs/manual/html/scalac.html
scalacOptions ++= List("-feature", "-deprecation", "-unchecked", "-Xlint")

// ScalaTest settings.
// Ignore tests tagged as @Slow (they should be picked only by integration test)
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-l",
  "org.scalatest.tags.Slow", "-u", "target/junit-xml-reports", "-oD", "-eS")

