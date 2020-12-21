name := "spark-records"
com.typesafe.sbt.SbtGit.versionWithGit

organization := "com.swoop"

bintrayOrganization := Some("swoop-inc")
bintrayPackageLabels := Seq("apache", "spark", "apache-spark", "scala", "big-data", "spark-records", "dataset", "swoop")
resolvers += "swoop-bintray" at "https://dl.bintray.com/swoop-inc/maven/"

licenses +=("Apache-2.0", url("http://apache.org/licenses/LICENSE-2.0"))

val vSpark = "3.0.1"

// Speed up dependency resolution (experimental)
updateOptions := updateOptions.value.withCachedResolution(true)

lazy val scalaSettings = Seq(
  scalaVersion := "2.12.12",
  crossScalaVersions := Seq("2.12.12"),
  scalacOptions in(Compile, doc) ++= Seq("-doc-root-content", baseDirectory.value + "/docs/root-doc.txt"),
  scalacOptions in(Compile, doc) ++= Seq("-groups", "-implicits"),
  javacOptions in(Compile, doc) ++= Seq("-notimestamp", "-linksource"),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
)

lazy val testSettings = Seq(
  testOptions in Test += Tests.Argument("-oS"),
  parallelExecution in Test := false,
  fork in Test := true,
)

lazy val sparkRecords = project
  .in(file("."))
  .settings(
    moduleName := "spark-records",
    name := moduleName.value,
    autoAPIMappings := true,
    scalaSettings,
    testSettings,
    headerLicense := Some(HeaderLicense.ALv2("2017", "Simeon Simeonov and Swoop, Inc.", HeaderLicenseStyle.SpdxSyntax)),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % vSpark % "provided" withSources(),
      "org.apache.spark" %% "spark-sql" % vSpark % "provided" withSources() excludeAll ExclusionRule(organization = "org.mortbay.jetty"),
      "org.apache.logging.log4j" % "log4j-core" % "2.7" % "provided" withSources(),
      "org.apache.logging.log4j" % "log4j-api" % "2.7" % "provided" withSources(),
      "org.scalatest" %% "scalatest" % "3.0.4" % "test" withSources()
    )
  )
  .enablePlugins(SiteScaladocPlugin)

lazy val docs = project
  .in(file("docs"))
  .settings(
    moduleName := "spark-records-docs",
    name := moduleName.value,
    scalaSettings,
    micrositeSettings,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % vSpark,
      "org.apache.spark" %% "spark-sql" % vSpark
    )
  )
  .dependsOn(sparkRecords)
  .enablePlugins(MicrositesPlugin)

lazy val micrositeSettings = Seq(
  micrositeCompilingDocsTool := WithTut,
  micrositeName := "Spark Records",
  micrositeDescription := "bulletproof Spark jobs",
  micrositeAuthor := "Swoop",
  micrositeHomepage := "http://www.swoop.com",
  micrositeBaseUrl := "spark-records",
  micrositeDocumentationUrl := "/spark-records/docs.html",
  micrositeGithubOwner := "swoop-inc",
  micrositeGithubRepo := "spark-records",
  micrositeHighlightTheme := "tomorrow"
)
