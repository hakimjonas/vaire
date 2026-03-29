ThisBuild / organization := "net.ghoula"
ThisBuild / scalaVersion := "3.8.2"
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision

val forgejoHost = sys.env.getOrElse("FORGEJO_HOST", "localhost")
val forgejoUrl = s"http://$forgejoHost:3000"
ThisBuild / resolvers ++= Seq(
  ("local-forgejo" at s"$forgejoUrl/api/packages/hakim/maven").withAllowInsecureProtocol(true)
)

// Java 25
ThisBuild / javacOptions ++= Seq("--release", "25")

// Compiler flags matching Eru
lazy val sharedScalacOptions = Seq(
  "-feature",
  "-Werror",
  "-Wunused:all",
  "-Wrecurse-with-default",
  "-no-indent",
  "-language:strictEquality",
  "-Yexplicit-nulls"
)

// Dependencies
val saratiVersion = "0.1.0+2-c539575a"
val rumilVersion = "0.2.0+4-6ac28897"
val sparkVersion = "4.1.1"

lazy val root = project
  .in(file("."))
  .aggregate(core, spark)
  .settings(
    name := "strongbow",
    publish / skip := true
  )

lazy val core = project
  .in(file("strongbow-core"))
  .settings(
    name := "strongbow-core",
    scalacOptions ++= sharedScalacOptions,
    libraryDependencies ++= Seq(
      "net.ghoula" %% "rumil-parsers" % rumilVersion,
      "net.ghoula" %% "sarati" % saratiVersion,
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      "org.scalacheck" %% "scalacheck" % "1.18.1" % Test
    )
  )

lazy val spark = project
  .in(file("strongbow-spark"))
  .dependsOn(core)
  .settings(
    name := "strongbow-spark",
    scalacOptions ++= sharedScalacOptions.filterNot(_ == "-language:strictEquality"),
    Test / scalacOptions ~= (_.map {
      case "-Wunused:all" => "-Wunused:imports"
      case other => other
    }),
    javacOptions := Seq("--release", "21"),
    libraryDependencies ++= Seq(
      ("org.apache.spark" %% "spark-sql" % sparkVersion % Provided)
        .cross(CrossVersion.for3Use2_13)
        .exclude("org.scala-lang.modules", "scala-xml_2.13"),
      ("org.apache.spark" %% "spark-sql" % sparkVersion % Test)
        .cross(CrossVersion.for3Use2_13)
        .exclude("org.scala-lang.modules", "scala-xml_2.13"),
      "org.scala-lang.modules" %% "scala-xml" % "2.4.0" % Test,
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),
    Test / fork := true,
    // Scala 3.8's unified scala-library uses TASTY metadata instead of ScalaSig annotations.
    // scala-reflect 2.13 (used by Spark internals) reads ScalaSig to resolve types like
    // Array.apply. Without ScalaSig, it fails: "class Array does not have a member apply".
    // Fix: prepend scala-library 2.13 to the forked test classpath so scala-reflect finds
    // ScalaSig metadata. The 2.13 classes are binary-compatible with 3.8; this only affects
    // the annotation format that scala-reflect reads.
    Test / fullClasspath := {
      val cp = (Test / fullClasspath).value
      val scalaReflectJar = cp
        .find(_.data.getName.startsWith("scala-reflect-"))
        .getOrElse(
          sys.error("scala-reflect jar not found on test classpath")
        )
      // Derive the 2.13.x version from the scala-reflect jar already on the classpath.
      // Coursier cache: .../org/scala-lang/scala-reflect/<ver>/ → .../org/scala-lang/scala-library/<ver>/
      val reflectVersion = scalaReflectJar.data.getName.stripPrefix("scala-reflect-").stripSuffix(".jar")
      val scalaLangDir = scalaReflectJar.data.getParentFile.getParentFile.getParentFile
      val scalaLib213 = Attributed.blank(
        scalaLangDir / "scala-library" / reflectVersion / s"scala-library-$reflectVersion.jar"
      )
      scalaLib213 +: cp
    },
    assembly / assemblyJarName := "strongbow-spark-bench.jar",
    assembly / mainClass := Some("net.ghoula.strongbow.spark.BenchRunner"),
    assembly / fullClasspath := (Test / fullClasspath).value,
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case PathList("META-INF", x, _*) if x.endsWith(".SF") || x.endsWith(".DSA") || x.endsWith(".RSA") =>
        MergeStrategy.discard
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*) => MergeStrategy.first
      case "module-info.class" => MergeStrategy.discard
      case x if x.endsWith(".class") => MergeStrategy.first
      case _ => MergeStrategy.first
    },
    Test / javaOptions ++= Seq(
      "-Xmx4G",
      "-Xss4M",
      "-XX:+UseZGC",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    ),
    Test / javaOptions ++= {
      sys.props.get("spark.test.master").map(v => s"-Dspark.test.master=$v").toSeq
    }
  )

// Command aliases
addCommandAlias("prepare", "scalafmtAll; scalafmtSbt; core/scalafixAll; Test/compile")
