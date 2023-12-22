name := "ora_ch_app"

ThisBuild / organization := "yakushev"
ThisBuild / version      := "0.0.2"
ThisBuild / scalaVersion := "2.13.10"

  val Versions = new {
    val clickhouseJdbc = "0.5.0"
    val slf4jApi       = "2.0.9"
    val log4jVers      = "2.0.9"
    val lz4Vers        = "1.8.0"
    val dbcp2Vers      = "2.9.0"
  }

  // PROJECTS
  lazy val global = project
  .in(file("."))
  .settings(commonSettings)
  .disablePlugins(AssemblyPlugin)
  .aggregate(
    ora_ch_app
  )

  lazy val ora_ch_app = (project in file("ora_ch_app"))
  .settings(
    Compile / mainClass        := Some("app.MainApp"),
    assembly / assemblyJarName := "ora_ch_app.jar",
    name := "ora_ch_app",
    commonSettings,
    libraryDependencies ++= commonDependencies
  )

  lazy val dependencies =
    new {
      val ch      = "com.clickhouse" % "clickhouse-jdbc"   % Versions.clickhouseJdbc
      val slf4j   = "org.slf4j" % "slf4j-api"              % Versions.slf4jApi
      val log4j   = "org.slf4j" % "slf4j-log4j12"          % Versions.log4jVers
      val lz4     = "org.lz4" % "lz4-java"                 % Versions.lz4Vers
      val dbcp2   = "org.apache.commons" % "commons-dbcp2" % Versions.dbcp2Vers
 
      val chDep = List(ch, slf4j, log4j, lz4)
    }

  val commonDependencies = {
    dependencies.chDep
  }

  lazy val compilerOptions = Seq(
          "-deprecation",
          "-encoding", "utf-8",
          "-explaintypes",
          "-feature",
          "-unchecked",
          "-language:postfixOps",
          "-language:higherKinds",
          "-language:implicitConversions",
          "-Xcheckinit",
          "-Xfatal-warnings",
          "-Ywarn-unused:params,-implicits"
  )

  lazy val commonSettings = Seq(
    scalacOptions ++= compilerOptions,
    resolvers ++= Seq(
      "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
      "Sonatype OSS Snapshots s01" at "https://s01.oss.sonatype.org/content/repositories/snapshots",
      Resolver.DefaultMavenRepository,
      Resolver.mavenLocal,
      Resolver.bintrayRepo("websudos", "oss-releases")
    )++
      Resolver.sonatypeOssRepos("snapshots")
     ++ Resolver.sonatypeOssRepos("public")
     ++ Resolver.sonatypeOssRepos("releases")
  )

  ora_ch_app / assembly / assemblyMergeStrategy := {
    case PathList("module-info.class") => MergeStrategy.discard
    case x if x.endsWith("/module-info.class") => MergeStrategy.discard
    case PathList("META-INF", xs @ _*)         => MergeStrategy.discard
    case "reference.conf" => MergeStrategy.concat
    case _ => MergeStrategy.first
  }