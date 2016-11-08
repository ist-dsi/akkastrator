organization := "pt.tecnico.dsi"
name := "akkastrator"

val javaVersion = "1.8"
initialize := {
  val current  = sys.props("java.specification.version")
  assert(current == javaVersion, s"Unsupported JDK: expected JDK $javaVersion installed, but instead got JDK $current.")
}
javacOptions ++= Seq(
  "-source", javaVersion,
  "-target", javaVersion,
  "-Xlint",
  "-encoding", "UTF-8"
)

scalaVersion := "2.12.0"
scalacOptions ++= Seq(
  "-deprecation", //Emit warning and location for usages of deprecated APIs.
  "-encoding", "UTF-8",
  "-feature", //Emit warning and location for usages of features that should be imported explicitly.
  "-language:implicitConversions", //Explicitly enables the implicit conversions feature
  "-unchecked", //Enable detailed unchecked (erasure) warnings
  "-Xfatal-warnings", //Fail the compilation if there are any warnings.
  "-Xlint", //Enable recommended additional warnings.
  "-Yno-adapted-args", //Do not adapt an argument list (either by inserting () or creating a tuple) to match the receiver.
  "-Ywarn-dead-code" //Warn when dead code is identified.
)

val akkaVersion = "2.4.12"
libraryDependencies ++= Seq(
  //Shapeless
  "com.chuusai" %% "shapeless" % "2.3.2",
  //Akka
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  //Persistence
  "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
  "org.iq80.leveldb" % "leveldb" % "0.9" % Test,
  "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8" % Test,
  //Logging
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0" % Test,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % Test,
  "ch.qos.logback" % "logback-classic" % "1.1.7" % Test,
  //Testing
  "org.scalatest" %% "scalatest" % "3.0.0" % Test,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,

  "commons-io" % "commons-io" % "2.5" % Test
)

//This is needed for LevelDB to work in tests
fork in Test := true

site.settings
site.includeScaladoc()
ghpages.settings
git.remoteRepo := s"git@github.com:ist-dsi/${name.value}.git"

homepage := Some(url(s"https://github.com/ist-dsi/${name.value}"))
licenses += "MIT" -> url("http://opensource.org/licenses/MIT")
scmInfo := Some(ScmInfo(homepage.value.get, s"git@github.com:ist-dsi/${name.value}.git"))

publishMavenStyle := true
publishTo := Some(if (isSnapshot.value) Opts.resolver.sonatypeSnapshots else Opts.resolver.sonatypeStaging)
publishArtifact in Test := false
sonatypeProfileName := organization.value

pomIncludeRepository := { _ => false }
pomExtra :=
  <developers>
    <developer>
      <id>Lasering</id>
      <name>Simão Martins</name>
      <url>https://github.com/Lasering</url>
    </developer>
  </developers>

import ReleaseTransformations._
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  ReleaseStep(action = Command.process("doc", _)),
  runTest,
  setReleaseVersion,
  tagRelease,
  ReleaseStep(action = Command.process("ghpagesPushSite", _)),
  ReleaseStep(action = Command.process("publishSigned", _)),
  ReleaseStep(action = Command.process("sonatypeRelease", _)),
  pushChanges,
  setNextVersion
)