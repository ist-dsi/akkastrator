logLevel := Level.Warn

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.7")

addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.3.4")

addSbtPlugin("com.typesafe.sbt" % "sbt-ghpages" % "0.6.2")

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.1.1")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "2.3")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")

addSbtPlugin("com.codacy" % "sbt-codacy-coverage" % "1.3.12")

// Removes some of the redundancy of the compiler output and prints additional info for implicit resolution errors.
addCompilerPlugin("io.tryp" % "splain" % "0.3.1" cross CrossVersion.patch)