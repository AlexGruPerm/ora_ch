name := "ora_ch_app"

ThisBuild / organization := "yakushev"
ThisBuild / version      := "0.0.9"
ThisBuild / scalaVersion := "2.13.10"

  val Versions = new {
    val ch_http_client     = "0.5.0"
    val apache_http_client = "5.3"
    val clickhouseJdbc     = "0.5.0"
    val slf4jApi           = "2.0.9"
    val log4jVers          = "2.0.9"
    val lz4Vers            = "1.8.0"
    val zio                = "2.0.20"
    val zio_config         = "4.0.0-RC16"
    val zio_http           = "3.0.0-RC4"
    val zio_json           = "0.6.2"
  }

  // PROJECTS
  lazy val global = project
  .in(file("."))
  .settings(commonSettings)
  .disablePlugins(AssemblyPlugin)
  .aggregate(
    ora_ch_app
  )

  unmanagedBase := baseDirectory.value / "lib"

  lazy val ora_ch_app = (project in file("ora_ch_app"))
  .settings(
    Compile / mainClass        := Some("app.MainApp"),
    Compile / unmanagedJars := (baseDirectory.value ** "*.jar").classpath,
    assembly / assemblyJarName := s"orach.jar",
    name := "ora_ch_app",
    commonSettings,
    libraryDependencies ++= commonDependencies
  )

  lazy val dependencies =
    new {
      val apacheHttpClient  = "org.apache.httpcomponents.client5" % "httpclient5" % Versions.apache_http_client
      val chHttpClient      = "com.clickhouse" % "clickhouse-http-client" % Versions.ch_http_client
      val ch                = "com.clickhouse" % "clickhouse-jdbc" % Versions.clickhouseJdbc classifier "all"
      val slf4j             = "org.slf4j" % "slf4j-api" % Versions.slf4jApi
      val log4j             = "org.slf4j" % "slf4j-log4j12" % Versions.log4jVers
      val lz4               = "org.lz4" % "lz4-java" % Versions.lz4Vers
      val zio               = "dev.zio" %% "zio" % Versions.zio
      val zio_conf          = "dev.zio" %% "zio-config" % Versions.zio_config
      val zio_conf_typesafe = "dev.zio" %% "zio-config-typesafe" % Versions.zio_config
      val zio_conf_magnolia = "dev.zio" %% "zio-config-magnolia" % Versions.zio_config
      val zio_http          = "dev.zio" %% "zio-http" % Versions.zio_http
      val zio_json          = "dev.zio" %% "zio-json" % Versions.zio_json

      val orai18n           = "com.oracle.database.nls" % "orai18n" % "23.3.0.23.09"

      val zioDep = List(zio, zio_conf, zio_conf_typesafe, zio_conf_magnolia, zio_http, zio_json)
      val chDep = List(chHttpClient,apacheHttpClient, ch, slf4j, log4j, lz4, orai18n)
    }

  val commonDependencies = {
    dependencies.zioDep ++ dependencies.chDep
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
    case "reference.conf" => MergeStrategy.concat
    case "META-INF/services/com.clickhouse.client.ClickHouseClient" => MergeStrategy.first
    case PathList("META-INF", xs @ _*)         => MergeStrategy.discard
    case _ => MergeStrategy.first
  }