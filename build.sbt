ThisBuild / version := "0.1.0-SNAPSHOT"

//changing from 2.11.12 to this broke the program. change was done to fix partitioned by query
//ThisBuild / scalaVersion := "2.12.13"
ThisBuild / scalaVersion := "2.11.12"


//changing from 2.4.8 to this broke the program. change was done to fix partitioned by query
//libraryDependencies ++= Seq(
//"org.apache.spark" %% "spark-core" % "3.0.1",
//"org.apache.spark" %% "spark-sql" % "3.0.1",
//"org.apache.spark" %% "spark-hive" % "3.0.1"
//)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-sql" % "2.4.8",
  "org.apache.spark" %% "spark-hive" % "2.4.8",
  "org.apache.poi" % "poi" % "5.2.0",
  "org.apache.poi" % "poi-ooxml" % "5.2.0",
  "org.apache.poi" % "poi-ooxml-lite" % "5.2.0",
  "com.github.mrpowers" %% "spark-daria" % "0.39.0",
  "org.vegas-viz" %% "vegas" % "0.3.9",
  "org.vegas-viz" %% "vegas-spark" % "0.3.9"
)


lazy val root = (project in file("."))
  .settings(
    name := "spark2demo"
  )
