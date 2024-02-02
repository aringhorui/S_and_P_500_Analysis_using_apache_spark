ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "BDMS"
  )
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.0",
      "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
      "org.apache.spark" %% "spark-mllib" % "3.5.0" % "provided",
      "com.mysql" % "mysql-connector-j" % "8.2.0",
      "mysql" % "mysql-connector-java" % "8.0.33",


    )
  )
