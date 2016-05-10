organization := "pt.tecnico.dsi"
name := "akkastrator"

initialize := {
  val required = "1.8"
  val current  = sys.props("java.specification.version")
  assert(current == required, s"Unsupported JDK: java.specification.version $current != $required")
}
javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

scalaVersion := "2.11.8"
scalacOptions ++= Seq(
  "-deprecation", //Emit warning and location for usages of deprecated APIs.
  "-encoding", "UTF-8",
  "-feature", //Emit warning and location for usages of features that should be imported explicitly.
  "-language:implicitConversions", //Explicitly enables the implicit conversions feature
  "-unchecked", //Enable detailed unchecked (erasure) warnings
  "-Xfatal-warnings", //Fail the compilation if there are any warnings.
  "-Xlint", //Enable recommended additional warnings.
  "-Yinline-warnings", //Emit inlining warnings.
  "-Yno-adapted-args", //Do not adapt an argument list (either by inserting () or creating a tuple) to match the receiver.
  "-Ywarn-dead-code" //Warn when dead code is identified.
)

val akkaVersion = "2.4.3"
libraryDependencies ++= Seq(
  //Akka
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  //Persistence
  "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
  "org.iq80.leveldb" % "leveldb" % "0.7" % Test,
  "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8" % Test,
  //Logging
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % Test,
  "ch.qos.logback" % "logback-classic" % "1.1.7" % Test,
  //Testing
  "org.scalatest" %% "scalatest" % "2.2.4" % Test,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test
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
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  ReleaseStep(action = Command.process("publishSigned", _)),
  ReleaseStep(action = Command.process("ghpagesPushSite", _)),
  setNextVersion,
  commitNextVersion,
  ReleaseStep(action = Command.process("sonatypeReleaseAll", _)),
  pushChanges
)