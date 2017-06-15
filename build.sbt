name := "squeryl-tools"

scalaVersion := "2.10.2"

version := "1.1-SNAPSHOT"

organization :="com.lunatech"

libraryDependencies ++= Seq(
  "org.squeryl" %% "squeryl" % "0.9.5-6",
  "postgresql" % "postgresql" % "9.1-901.jdbc4",
  "mysql" % "mysql-connector-java" % "5.1.25",
  "org.slf4j" % "slf4j-api" % "1.7.5",
  "org.specs2" %% "specs2" % "2.2.2" % "test")

publishTo <<= version { (v: String) =>
  val path = if(v.trim.endsWith("SNAPSHOT")) "snapshots" else "releases"
  Some(Resolver.url("Public Lunatech Artifactory", new URL("http://artifactory.lunatech.com/artifactory/%s-public/" format path)))
}
