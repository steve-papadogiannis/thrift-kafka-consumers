import sbt.Keys._
import sbt._

object ProjectSettings {
  def root(): Project = Project("thrift-kafka-consumers-all", file("modules/thrift-kafka-consumers-all"))
    .settings(commonSettings)

  def library(applicationName: String): Project = {
    Project(applicationName, file("modules/" + applicationName))
      .settings(commonSettings)
  }

  private lazy val commonSettings = Seq(
    organization := "gr.papadogiannis.stefanos",
    version := "1.0-SNAPSHOT",
    scalaVersion := "2.13.8",
    scalacOptions ++= commonScalacOptions,
    Compile / doc / sources := Seq.empty,
    Compile / packageDoc / publishArtifact := false,
    updateOptions := updateOptions.value.withCachedResolution(true),
    Global / cancelable := true,
    resolvers ++= Seq(
      Resolver.mavenLocal,
      "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases",
      "Sonatype releases" at "https://oss.sonatype.org/content/repositories/releases"
    )
  )

  private lazy val commonScalacOptions = Seq(
    "-encoding",
    "UTF-8",
    "-feature",
    "-language:implicitConversions",
    "-language:postfixOps",
    "-unchecked",
    "-Ywarn-dead-code",
    "-Yrangepos",
    "-Xfuture"
  )
}
