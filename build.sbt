import java.net.URL

import sbt.{Developer, ScmInfo}
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import xerial.sbt.Sonatype.SonatypeCommand

organization := "pt.tecnico.dsi"
name := "akkastrator"

//======================================================================================================================
//==== Compile Options =================================================================================================
//======================================================================================================================
javacOptions ++= Seq("-Xlint", "-encoding", "UTF-8", "-Dfile.encoding=utf-8")
scalaVersion := "2.12.5"
crossScalaVersions := Seq("2.11.12", scalaVersion.value, "2.13.0-M3")

scalacOptions ++= Seq(
  "-deprecation",                      // Emit warning and location for usages of deprecated APIs.
  "-encoding", "utf-8",                // Specify character encoding used by source files.
  "-explaintypes",                     // Explain type errors in more detail.
  "-feature",                          // Emit warning and location for usages of features that should be imported explicitly.
  "-language:implicitConversions",     // Explicitly enables the implicit conversions feature
  "-unchecked",                        // Enable additional warnings where generated code depends on assumptions.
  //"-Xcheckinit",                       // Wrap field accessors to throw an exception on uninitialized access.
  "-Xfatal-warnings",                  // Fail the compilation if there are any warnings.
  "-Xlint",                            // Enables every warning. scala -Xlint:help for a list and explanation
  "-Yno-adapted-args",                 // Do not adapt an argument list (either by inserting () or creating a tuple) to match the receiver.
  "-Ypartial-unification",             // Enable partial unification in type constructor inference
  "-Ywarn-dead-code",                  // Warn when dead code is identified.
  "-Ywarn-inaccessible",               // Warn about inaccessible types in method signatures.
  "-Ywarn-infer-any",                  // Warn when a type argument is inferred to be `Any`.
  "-Ywarn-nullary-override",           // Warn when non-nullary `def f()' overrides nullary `def f'.
  "-Ywarn-nullary-unit",               // Warn when nullary methods return Unit.
  "-Ywarn-numeric-widen",              // Warn when numerics are widened.
  //"-Ywarn-value-discard",              // Warn when non-Unit expression results are unused.
) ++ (CrossVersion.partialVersion(scalaVersion.value) match {
  case Some((2, 12)) => Seq(
    "-Ywarn-extra-implicit",             // Warn when more than one implicit parameter section is defined.
    "-Ywarn-unused:imports",             // Warn if an import selector is not referenced.
    "-Ywarn-unused:privates",            // Warn if a private member is unused.
    "-Ywarn-unused:locals",              // Warn if a local definition is unused.
    "-Ywarn-unused:implicits",           // Warn if an implicit parameter is unused.
    "-Ywarn-unused:params",              // Warn if a value parameter is unused.
    "-Ywarn-unused:patvars",             // Warn if a variable bound in a pattern is unused.
  )
  case _ => Nil
})

// These lines ensure that in sbt console or sbt test:console the -Ywarn* and the -Xfatal-warning are not bothersome.
// https://stackoverflow.com/questions/26940253/in-sbt-how-do-you-override-scalacoptions-for-console-in-all-configurations
scalacOptions in (Compile, console) ~= (_ filterNot { option =>
  option.startsWith("-Ywarn") || option == "-Xfatal-warnings"
})
scalacOptions in (Test, console) := (scalacOptions in (Compile, console)).value

//======================================================================================================================
//==== Dependencies ====================================================================================================
//======================================================================================================================
val akkaVersion = "2.5.12"
libraryDependencies ++= Seq(
  //Config
  "com.typesafe" % "config" % "1.3.3",
  //Shapeless
  "com.chuusai" %% "shapeless" % "2.3.3",
  //Akka
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  //Persistence
  "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
  "org.iq80.leveldb" % "leveldb" % "0.10" % Test,
  "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8" % Test,
  //Logging
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0" % Test,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % Test,
  "ch.qos.logback" % "logback-classic" % "1.2.3" % Test,
  //Testing
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,

  "commons-io" % "commons-io" % "2.6" % Test
)

// This is needed for LevelDB to work in tests
fork in Test := true

//======================================================================================================================
//==== Scaladoc ========================================================================================================
//======================================================================================================================
val latestReleasedVersion = SettingKey[String]("latest released version")
git.useGitDescribe := true
latestReleasedVersion := git.gitDescribedVersion.value.getOrElse("0.1.0")

autoAPIMappings := true //Tell scaladoc to look for API documentation of managed dependencies in their metadata.
scalacOptions in (Compile, doc) ++= Seq(
  "-diagrams",    // Create inheritance diagrams for classes, traits and packages.
  "-groups",      // Group similar functions together (based on the @group annotation)
  "-implicits",   // Document members inherited by implicit conversions.
  "-doc-title", name.value.capitalize,
  "-doc-version", latestReleasedVersion.value,
  "-doc-source-url", s"${homepage.value.get}/tree/v${latestReleasedVersion.value}€{FILE_PATH}.scala",
  "-sourcepath", (baseDirectory in ThisBuild).value.getAbsolutePath
)
//Define the base URL for the Scaladocs for your library. This will enable clients of your library to automatically
//link against the API documentation using autoAPIMappings.
apiURL := Some(url(s"${homepage.value.get}/${latestReleasedVersion.value}/api/"))

enablePlugins(GhpagesPlugin)
siteSubdirName in SiteScaladoc := s"api/${version.value}"
envVars in ghpagesPushSite := Map("SBT_GHPAGES_COMMIT_MESSAGE" -> s"Add Scaladocs for version ${latestReleasedVersion.value}")
git.remoteRepo := s"git@github.com:ist-dsi/${name.value}.git"

//======================================================================================================================
//==== Deployment ======================================================================================================
//======================================================================================================================
//publishTo := Some(if (isSnapshot.value) Opts.resolver.sonatypeSnapshots else Opts.resolver.sonatypeStaging)
//sonatypeProfileName := organization.value
publishTo := sonatypePublishTo.value

licenses += "MIT" -> url("http://opensource.org/licenses/MIT")
homepage := Some(url(s"https://github.com/ist-dsi/${name.value}"))
scmInfo := Some(ScmInfo(homepage.value.get, git.remoteRepo.value))
developers += Developer("Lasering", "Simão Martins", "", new URL("https://github.com/Lasering"))

// Will fail the build/release if updates for the dependencies are found
dependencyUpdatesFailBuild := true

coverageFailOnMinimum := true
coverageMinimum := 95

releaseCrossBuild := true
releasePublishArtifactsAction := PgpKeys.publishSigned.value
releaseProcess := Seq[ReleaseStep](
  releaseStepTask(dependencyUpdates),
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  releaseStepTask(doc),
  runTest,
  setReleaseVersion,
  tagRelease,
  releaseStepTask(ghpagesPushSite),
  publishArtifacts,
  releaseStepCommand(SonatypeCommand.sonatypeReleaseAll),
  pushChanges,
  setNextVersion
)
