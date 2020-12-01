name := "spark-records"
com.typesafe.sbt.SbtGit.versionWithGit

organization := "com.swoop"

bintrayOrganization := Some("swoop-inc")
bintrayPackageLabels := Seq("apache", "spark", "apache-spark", "scala", "big-data", "spark-records", "dataset", "swoop")
resolvers += "swoop-bintray" at "https://dl.bintray.com/swoop-inc/maven/"

licenses +=("Apache-2.0", url("http://apache.org/licenses/LICENSE-2.0"))

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

scalaVersion := "2.12.1"

crossScalaVersions := Seq("2.12.1")

val vSpark = "3.0.1"

// Speed up dependency resolution (experimental)
updateOptions := updateOptions.value.withCachedResolution(true)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % vSpark % "provided" withSources(),
  "org.apache.spark" %% "spark-sql" % vSpark % "provided" withSources() excludeAll ExclusionRule(organization = "org.mortbay.jetty"),
  "org.apache.logging.log4j" % "log4j-core" % "2.7" % "provided" withSources(),
  "org.apache.logging.log4j" % "log4j-api" % "2.7" % "provided" withSources(),
  "org.scalatest" %% "scalatest" % "3.0.4" % "test" withSources()
)

testOptions in Test += Tests.Argument("-oS")
parallelExecution in Test := false
fork in Test := true

// @see https://wiki.scala-lang.org/display/SW/Configuring+SBT+to+Generate+a+Scaladoc+Root+Page
scalacOptions in(Compile, doc) ++= Seq("-doc-root-content", baseDirectory.value + "/docs/root-doc.txt")
scalacOptions in(Compile, doc) ++= Seq("-groups", "-implicits")
javacOptions in(Compile, doc) ++= Seq("-notimestamp", "-linksource")
autoAPIMappings := true

enablePlugins(MicrositesPlugin)
enablePlugins(SiteScaladocPlugin)

val docsDir = "docs"
tutSourceDirectory := baseDirectory.value / docsDir / "main" / "tut"
micrositeImgDirectory := baseDirectory.value / docsDir / "main" / "resources" / "site" / "images"
micrositeCssDirectory := baseDirectory.value / docsDir / "main" / "resources" / "site" / "styles"
micrositeJsDirectory := baseDirectory.value / docsDir / "main" / "resources" / "site" / "scripts"
micrositeName := "Spark Records"
micrositeDescription := "bulletproof Spark jobs"
micrositeAuthor := "Swoop"
micrositeHomepage := "http://www.swoop.com"
micrositeBaseUrl := "spark-records"
micrositeDocumentationUrl := "/spark-records/docs.html"
micrositeGithubOwner := "swoop-inc"
micrositeGithubRepo := "spark-records"
micrositeHighlightTheme := "tomorrow"

headerLicense := Some(HeaderLicense.ALv2("2017", "Simeon Simeonov and Swoop, Inc.", HeaderLicenseStyle.SpdxSyntax))

