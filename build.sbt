ThisBuild / organization := "net.ghoula"
ThisBuild / scalaVersion := "3.8.4"
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision

ThisBuild / licenses := List("MIT" -> uri("https://opensource.org/licenses/MIT"))
ThisBuild / homepage := Some(uri("https://codeberg.org/hakim/strongbow"))
ThisBuild / description := "A type-safe columnar dataset library for Scala 3 with Spark integration"
ThisBuild / developers := List(
  Developer(
    id = "hakimjonas",
    name = "Hakim Jonas Ghoula",
    email = "hakim@ghoula.net",
    url = uri("https://codeberg.org/hakim")
  )
)
ThisBuild / scmInfo := Some(
  ScmInfo(
    uri("https://codeberg.org/hakim/strongbow"),
    "scm:git@codeberg.org:hakim/strongbow.git"
  )
)

// ===== Publishing Settings =====
val forgejoHost = sys.env.getOrElse("FORGEJO_HOST", "localhost")
val forgejoUrl = s"http://$forgejoHost:3000"

ThisBuild / publishTo := {
  if (sys.env.contains("CODEBERG_TOKEN"))
    Some("codeberg" at "https://codeberg.org/api/packages/hakim/maven")
  else
    Some(("local-forgejo" at s"$forgejoUrl/api/packages/hakim/maven").withAllowInsecureProtocol(true))
}
ThisBuild / publishMavenStyle := true
ThisBuild / Test / publishArtifact := false

ThisBuild / resolvers ++= Seq(
  "codeberg" at "https://codeberg.org/api/packages/hakim/maven",
  ("local-forgejo" at s"$forgejoUrl/api/packages/hakim/maven").withAllowInsecureProtocol(true)
)

ThisBuild / credentials ++= sys.env
  .get("CODEBERG_TOKEN")
  .map(token => Credentials("Gitea Package API", "codeberg.org", "hakim", token))
  .toSeq

ThisBuild / credentials ++= sys.env
  .get("FORGEJO_TOKEN")
  .map(token => Credentials("Gitea Package API", forgejoHost, "hakim", token))
  .toSeq

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
val saratiVersion = "0.3.12"
val rumilVersion = "0.3.12"
val sparkVersion = "4.2.0"

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
      "org.scalatest" %% "scalatest" % "3.2.20" % Test,
      "org.scalacheck" %% "scalacheck" % "1.20.0" % Test
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
      "org.scalatest" %% "scalatest" % "3.2.20" % Test
    ),
    Test / fork := true,
    Test / parallelExecution := false,
    Test / testOptions += Tests.Argument("-l", "net.ghoula.strongbow.Benchmark"),
    Test / testOptions ++= {
      if (sys.env.contains("FAST_TESTS"))
        Seq(Tests.Argument("-l", "net.ghoula.strongbow.Slow"))
      else Seq.empty
    },
    // Scala 3.8's unified scala-library uses TASTY metadata instead of ScalaSig annotations.
    // scala-reflect 2.13 (used by Spark internals) reads ScalaSig to resolve types like
    // Array.apply. Without ScalaSig, it fails: "class Array does not have a member apply".
    // Fix: prepend scala-library 2.13 to the forked test classpath so scala-reflect finds
    // ScalaSig metadata. The 2.13 classes are binary-compatible with 3.8; this only affects
    // the annotation format that scala-reflect reads.
    Test / fullClasspath := {
      val cp = (Test / fullClasspath).value
      val converter = fileConverter.value
      val scalaReflectJar = cp
        .find(_.data.name.startsWith("scala-reflect-"))
        .getOrElse(
          sys.error("scala-reflect jar not found on test classpath")
        )
      // Derive the 2.13.x version from the scala-reflect jar already on the classpath.
      // Coursier cache: .../org/scala-lang/scala-reflect/<ver>/ → .../org/scala-lang/scala-library/<ver>/
      val reflectVersion = scalaReflectJar.data.name.stripPrefix("scala-reflect-").stripSuffix(".jar")
      val scalaLangDir = converter.toPath(scalaReflectJar.data).getParent.getParent.getParent
      val scalaLib213 = Attributed.blank[xsbti.HashedVirtualFileRef](
        converter.toVirtualFile(
          scalaLangDir.resolve("scala-library").resolve(reflectVersion).resolve(s"scala-library-$reflectVersion.jar")
        )
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
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    ),
    Test / javaOptions ++= {
      sys.props.get("spark.test.master").map(v => s"-Dspark.test.master=$v").toSeq
    }
  )

// Command aliases
addCommandAlias("prepare", "scalafmtAll; scalafmtSbt; core/scalafixAll; Test/compile")
addCommandAlias("check", "core/scalafixAll --check; scalafmtCheckAll; scalafmtSbtCheck")
addCommandAlias("testAll", "core/Test/testFull; spark/Test/testFull")
addCommandAlias("testSlow", "spark/Test/testOnly * -- -n net.ghoula.strongbow.Slow")
