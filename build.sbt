ThisBuild / organization := "net.ghoula"
ThisBuild / scalaVersion := "3.7.4"
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision

// Java 21
ThisBuild / javacOptions ++= Seq("--release", "21")

// Compiler flags matching Eru
lazy val sharedScalacOptions = Seq(
  "-feature",
  "-Xfatal-warnings",
  "-Wunused:all",
  "-Wrecurse-with-default",
  "-no-indent",
  "-language:strictEquality"
)

lazy val testScalacOptions = Seq(
  "-Wunused:imports"
)

// Dependencies
val valarVersion = "0.1.0-SNAPSHOT"
val rumilVersion = "0.1.0-SNAPSHOT"
val eruVersion = "0.1.0-SNAPSHOT"
val sparkVersion = "4.1.0"

lazy val root = project
  .in(file("."))
  .aggregate(core, columnar, spark, bench)
  .settings(
    name := "strongbow",
    publish / skip := true
  )

lazy val core = project
  .in(file("strongbow-core"))
  .settings(
    name := "strongbow-core",
    scalacOptions ++= sharedScalacOptions,
    // ⚡ ZERO dependencies—only Scala stdlib
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.18" % Test,
      "org.scalacheck" %% "scalacheck" % "1.17.0" % Test
    )
  )

lazy val columnar = project
  .in(file("strongbow-columnar"))
  .dependsOn(core)
  .settings(
    name := "strongbow-columnar",
    scalacOptions ++= sharedScalacOptions
    // No extra dependencies—just core
  )

lazy val spark = project
  .in(file("strongbow-spark"))
  .dependsOn(core)
  .settings(
    name := "strongbow-spark",
    scalacOptions ++= sharedScalacOptions.filterNot(o => o == "-language:strictEquality" || o == "-Wunused:all"),
    scalacOptions += "-Wunused:imports",
    libraryDependencies ++= Seq(
      ("org.apache.spark" %% "spark-sql" % sparkVersion % Provided)
        .cross(CrossVersion.for3Use2_13)
        .exclude("org.scala-lang.modules", "scala-xml_2.13"),
      ("org.apache.spark" %% "spark-sql" % sparkVersion % Test)
        .cross(CrossVersion.for3Use2_13)
        .exclude("org.scala-lang.modules", "scala-xml_2.13"),
      "org.scala-lang.modules" %% "scala-xml" % "2.3.0" % Test,
      "org.scalatest" %% "scalatest" % "3.2.18" % Test
    ),
    Test / fork := true,
    Test / javaOptions ++= Seq(
      "-Xms8G",
      "-Xmx48G",
      "-Xss4M",
      "-XX:+UseZGC",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED"
    )
  )

// Optional integration modules (commented out until dependencies are published):
//
// lazy val validation = project
//   .in(file("strongbow-validation"))
//   .dependsOn(core)
//   .settings(
//     name := "strongbow-validation",
//     scalacOptions ++= sharedScalacOptions,
//     libraryDependencies ++= Seq(
//       "net.ghoula" %% "valar-core" % valarVersion
//     )
//   )
//
// lazy val io = project
//   .in(file("strongbow-io"))
//   .dependsOn(core, columnar)
//   .settings(
//     name := "strongbow-io",
//     scalacOptions ++= sharedScalacOptions,
//     libraryDependencies ++= Seq(
//       "net.ghoula" %% "rumil-core" % rumilVersion,
//       "net.ghoula" %% "eru-core" % eruVersion
//     )
//   )
//
// lazy val effects = project
//   .in(file("strongbow-effects"))
//   .dependsOn(core, columnar)
//   .settings(
//     name := "strongbow-effects",
//     scalacOptions ++= sharedScalacOptions,
//     libraryDependencies ++= Seq(
//       "net.ghoula" %% "eru-core" % eruVersion
//     )
//   )

lazy val bench = project
  .in(file("strongbow-bench"))
  .dependsOn(core, columnar)
  .settings(
    name := "strongbow-bench",
    scalacOptions ++= testScalacOptions,
    publish / skip := true,
    fork := true,
    javaOptions ++= Seq("-Xms8G", "-Xmx48G", "-Xss4M", "-XX:+UseZGC"),
    // Benchmarks excluded from scalafix - performance code may use vars/unsafe patterns
    scalafixOnCompile := false
  )

// Command aliases
addCommandAlias("prepare", "scalafmtAll; scalafmtSbt; core/scalafixAll; columnar/scalafixAll; Test/compile")
