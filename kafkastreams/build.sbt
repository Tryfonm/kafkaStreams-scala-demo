
scalaVersion := "2.13.8"

name := "kafkaSTreams"
organization := "ch.epfl.scala"
version := "1.0"



libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "2.1.1"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "2.8.0",
  "org.apache.kafka" % "kafka-streams" % "2.8.0",
  "org.apache.kafka" %% "kafka-streams-scala" % "2.8.0",
  "io.circe" %% "circe-core" % "0.14.1",
  "io.circe" %% "circe-generic" % "0.14.1",
  "io.circe" %% "circe-parser" % "0.14.1"
)