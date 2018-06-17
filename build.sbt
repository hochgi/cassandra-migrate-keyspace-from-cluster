import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "hochgi",
      scalaVersion := "2.12.6",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "cassandra-migrate-keyspace-from-cluster",
    fork := true,
    libraryDependencies ++= Seq(
      scalaTest % Test,
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "com.typesafe" % "config" % "1.3.3",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
      "com.lightbend.akka" %% "akka-stream-alpakka-cassandra" % "0.19",
      "nl.grons" %% "metrics4-scala" % "4.0.1",
      "nl.grons" %% "metrics4-akka_a25" % "4.0.1",
      "nl.grons" %% "metrics4-scala-hdr" % "4.0.1",
      "io.dropwizard.metrics" % "metrics-jmx" % "4.0.2"
    ),
    javaOptions ++= Seq(
      "-Dcom.sun.management.jmxremote.port=1729",
      "-Dcom.sun.management.jmxremote.authenticate=false",
      "-Dcom.sun.management.jmxremote.ssl=false"
    )
  )
