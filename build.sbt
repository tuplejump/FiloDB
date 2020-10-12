import sbt._
import sbt.Keys._

publishTo := Some(Resolver.file("Unused repo", file("target/unusedrepo")))

// Global setting across all subprojects
ThisBuild / organization := "org.filodb"
ThisBuild / organizationName := "FiloDB"
ThisBuild / scalaVersion := "2.12.12"
ThisBuild / publishMavenStyle := true
ThisBuild / Test / publishArtifact := false
ThisBuild / IntegrationTest / publishArtifact := false
ThisBuild / licenses += ("Apache-2.0", url("http://choosealicense.com/licenses/apache/"))
ThisBuild / pomIncludeRepository := { x => false }

lazy val memory = Submodules.memory
lazy val core = Submodules.core
lazy val query = Submodules.query
lazy val prometheus = Submodules.prometheus
lazy val coordinator = Submodules.coordinator
lazy val cassandra = Submodules.cassandra
lazy val kafka = Submodules.kafka
lazy val cli = Submodules.cli
lazy val http = Submodules.http
lazy val gateway = Submodules.gateway
lazy val standalone = Submodules.standalone
lazy val bootstrapper = Submodules.bootstrapper
lazy val sparkJobs = Submodules.sparkJobs
lazy val jmh = Submodules.jmh


