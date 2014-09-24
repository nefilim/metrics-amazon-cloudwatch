name := "metrics-amazon-cloudwatch"

organization := "com.pongr"

scalaVersion := "2.10.4"

resolvers ++= Seq(
  "Sonatype" at "https://oss.sonatype.org/content/groups/public",
  "typesafe repo"   at "http://repo.typesafe.com/typesafe/releases/"
)

libraryDependencies ++= Seq(
  "org.specs2" %% "specs2" % "2.3.10" % "test",
  "org.mockito" % "mockito-all" % "1.9.5" % "test",
  "nl.grons" %% "metrics-scala" % "3.3.0",
  "com.amazonaws" % "aws-java-sdk" % "1.8.10.2",
  "com.typesafe.scala-logging"     %% "scala-logging-slf4j"       % "2.1.2"
)

releaseSettings

//http://www.scala-sbt.org/using_sonatype.html
//https://docs.sonatype.org/display/Repository/Sonatype+OSS+Maven+Repository+Usage+Guide
//publishTo <<= version { v: String =>
//  val nexus = "https://oss.sonatype.org/"
//  if (v.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus + "content/repositories/snapshots/")
//  else                             Some("releases" at nexus + "service/local/staging/deploy/maven2/")
//}
publishTo <<= version { v: String =>
  val nexus = "https://nexus.playmino.com/nexus/"
  if (v.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "content/repositories/releases")
}

publishMavenStyle := true

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

homepage := Some(url("http://github.com/pongr/metrics-amazon-cloudwatch"))

organizationName := "Pongr"

organizationHomepage := Some(url("http://pongr.com"))

description := "Amazon Cloud watch reporter for metrics"

pomExtra := (
  <scm>
    <url>git@github.com:pongr/metrics-amazon-cloudwatch.git</url>
    <connection>scm:git:git@github.com:pongr/metrics-amazon-cloudwatch.git</connection>
  </scm>
  <developers>
    <developer>
      <id>pcetsogtoo</id>
      <name>Byamba Tumurkhuu</name>
      <url>https://github.com/pcetsogtoo</url>
    </developer>
    <developer>
      <id>zcox</id>
      <name>Zach Cox</name>
      <url>https://github.com/zcox</url>
    </developer>
    <developer>
      <id>nefilim</id>
      <name>Peter vR</name>
      <url>https://github.com/nefilim</url>
    </developer>
  </developers>
)
