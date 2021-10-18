name := "Orchestrator"

version := "1.0"

scalaVersion := "2.11.8"
scapegoatVersion in ThisBuild := "1.1.0"
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

logLevel := sbt.Level.Error

mainClass := Some("com.tfs.orchestrator.Main")

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-hdfs" % "2.8.0" % "provided",
  "org.apache.hadoop" % "hadoop-common" % "2.8.0" % "provided",
  "org.scala-lang" % "scala-library" % "2.11.8",
  "org.apache.oozie" % "oozie-client" % "4.3.0" % "provided",
  "mysql" % "mysql-connector-java" % "8.0.13",
  "org.hibernate" % "hibernate-core" % "4.1.8.Final",
  "org.hibernate" % "hibernate-entitymanager" % "4.1.8.Final",
  "org.hibernate" % "hibernate-validator" % "4.3.0.Final",
  "org.hibernate" % "hibernate-c3p0" % "4.1.8.Final",
  "org.apache.logging.log4j" %% "log4j-api-scala" % "11.0",
  "org.apache.logging.log4j" % "log4j-api" % "2.8.2",
  "org.apache.logging.log4j" % "log4j-core" % "2.8.2" % Runtime,
  "org.scalaj" %% "scalaj-http" % "2.3.0",
  "net.liftweb" %% "lift-json" % "3.1.1",
  "io.undertow" % "undertow-core" % "1.4.20.Final",
  "org.scalactic" %% "scalactic" % "3.0.4" % Test,
  "org.scalatest" %% "scalatest" % "3.0.4" % Test,
  "org.scalamock" %% "scalamock" % "4.0.0" % Test,
  "org.quartz-scheduler" % "quartz" % "2.3.0",
  "com.h2database" % "h2" % "1.4.197",
  "org.apache.kafka" % "kafka-clients" % "0.10.0.0",
  "org.mockito" % "mockito-core" % "2.7.19" % Test
)

excludeDependencies ++= Seq(
	"com.mchange" % "c3p0",
	"com.mchange" % "mchange-commons-java"
)

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case PathList(ps @ _*) if ps.head endsWith ".properties" => MergeStrategy.discard
  case PathList("properties", xs @ _*)                 => MergeStrategy.discard
  case PathList("templates", xs @ _*)                 => MergeStrategy.discard
  case PathList("scripts", xs @ _*)                 => MergeStrategy.discard
  case PathList("samples", xs @ _*)                 => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

logBuffered in Test := false
retrieveManaged := true
mainClass in assembly := Some("com.tfs.orchestrator.Main")
parallelExecution in Test := false

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-f", "result.txt", "-eNDXEHLO")

