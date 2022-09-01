import ProjectSettings.{library, root}

val AkkaVersion = "2.6.15"
val JacksonVersion = "2.11.4"
val TwitterLibVersion = "21.9.0"
val ThriftVersion = "0.10.0"
/**
 * Starting external dep version declaration section
 * Example: val SomeExternalDepVersion = "0.0.0"
 */
// TODO: Add external dep versions
/**
 * Ending external dep version declaration section
 */

val AkkaKafkaStreams = "com.typesafe.akka" %% "akka-stream-kafka" % "2.1.1"
val AkkaStreams = "com.typesafe.akka" %% "akka-stream" % AkkaVersion
val JacksonDatabind = "com.fasterxml.jackson.core" % "jackson-databind" % JacksonVersion
val Slf4jApi = "org.slf4j" % "slf4j-api" % "1.7.32"
val Slf4jSimple = "org.slf4j" % "slf4j-simple" % "1.7.32"

val Thrift = "org.apache.thrift" % "libthrift" % ThriftVersion excludeAll(
  ExclusionRule("org.apache.httpcomponents", "httpclient"),
  ExclusionRule("org.apache.httpcomponents", "httpcore"),
  ExclusionRule("org.slf4j", "slf4j-api")
)

val Scrooge = Seq(
  "com.twitter" %% "scrooge-core" % TwitterLibVersion exclude("com.twitter", "libthrift"),
  "com.twitter" %% "scrooge-generator" % TwitterLibVersion % "provided" exclude("com.twitter", "libthrift")
)

val Finagle = Seq(
  "com.twitter" %% "finagle-core" % TwitterLibVersion exclude("com.twitter", "libthrift"),
  "com.twitter" %% "finagle-http" % TwitterLibVersion exclude("com.twitter", "libthrift"),
  "com.twitter" %% "finagle-thrift" % TwitterLibVersion exclude("com.twitter", "libthrift"),
  "com.twitter" %% "finagle-thriftmux" % TwitterLibVersion exclude("com.twitter", "libthrift")
)

/**
 * Starting external dep declaration section
 * Example: val SomeExternalDep = "some.domain" % "some-external-dep" % SomeExternalDepVersion
 */
// TODO: Add external deps
/**
 * Ending external dep declaration section
 */

val thriftIDLDependencies = library("thrift-idl")

  /**
   * Starting external dep libraryDependencies declaration section
   * Example: .settings(libraryDependencies ++= Seq(SomeExternalDep))
   */
  // TODO: Add to libraryDependencies
  /**
   * Ending external dep declaration section
   */

  .enablePlugins(ScroogeSBT)
  .settings(dependencyOverrides += "org.apache.thrift" % "libthrift" % ThriftVersion)
  .settings(libraryDependencies ++= Scrooge)
  .settings(libraryDependencies ++= Finagle)
  .settings(libraryDependencies += Thrift)

  /**
   * Starting scroogeThriftDependencies declaration section
   * Example: .settings(Compile / scroogeThriftDependencies := Seq("some-spec-idl"))
   */
  // TODO: Add to scroogeThriftDependencies
  /**
   * Ending scroogeThriftDependencies declaration section
   */

  .settings(Compile / scroogeThriftSources ++= {
    (Compile / scroogeUnpackDeps).value.flatMap { dir => (dir ** "*.thrift").get }
  })

val thriftDependencies = library("thrift")
  .settings(
    Compile / unmanagedJars += (file(
      "modules"
    ) / "thrift-idl" / "target" / s"scala-${scalaBinaryVersion.value}" / s"thrift-idl_${scalaBinaryVersion.value}-1.0-SNAPSHOT.jar").getAbsoluteFile
  )

val thriftKafkaConsumers = root()
  .dependsOn(thriftDependencies)
  .aggregate(thriftDependencies)
  .settings(libraryDependencies ++= Seq(AkkaKafkaStreams, AkkaStreams, JacksonDatabind, Thrift, Slf4jApi, Slf4jSimple) ++ Scrooge)
