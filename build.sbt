name := "squeryl-tools"

scalaVersion := "2.10.2"

libraryDependencies ++= Seq(
  "org.squeryl" %% "squeryl" % "0.9.5-6",
  "postgresql" % "postgresql" % "9.1-901.jdbc4",
  "org.specs2" %% "specs2" % "2.2.2" % "test")
