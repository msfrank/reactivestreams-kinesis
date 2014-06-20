import sbt._
import Keys._

object ReactivestreamsKinesisBuild extends Build {

  val reactivestreamsKinesisVersion = "0.0.1"

  val scalaLangVersion = "2.10.4"
  val akkaVersion = "2.3.3"

  lazy val reactivestreamsKinesisBuild = Project(
    id = "reactivestreams-kinesis",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(

      exportJars := true,
      name := "reactivestreams-kinesis",
      version := reactivestreamsKinesisVersion,
      scalaVersion := scalaLangVersion,
      javacOptions ++= Seq("-source", "1.7"),

      libraryDependencies ++= Seq(
        "com.amazonaws" % "amazon-kinesis-client" % "1.0.0",
        "org.reactivestreams" % "reactive-streams" % "0.4.0.M1",
        "org.slf4j" % "slf4j-api" % "1.7.5",
        // dependencies for testing
        "org.reactivestreams" % "reactive-streams-tck" % "0.4.0.M1" % "test",
        //"com.typesafe.akka" %% "akka-stream-experimental" % "0.3" % "test",
        "com.typesafe.akka" %% "akka-actor" % akkaVersion % "test",
        "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % "test",
        "org.scalatest" %% "scalatest" % "1.9.1" % "test",
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
        "ch.qos.logback" % "logback-classic" % "1.1.2" % "test"
      )
    )
  )
}
