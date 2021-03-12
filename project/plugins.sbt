scalaVersion := "2.12.12"

resolvers += "jgit-repo" at "https://download.eclipse.org/jgit/maven"

addSbtPlugin("com.47deg"  % "sbt-microsites" % "1.1.5")
addSbtPlugin("com.typesafe.sbt" % "sbt-ghpages" % "0.6.3")
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "1.0.0")
addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.6.0")
