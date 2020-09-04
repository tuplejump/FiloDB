import sbt._
import Keys._
import sbt.librarymanagement.ScalaModuleInfo

import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm
import sbt.Tests.Output._
import org.scalastyle.sbt.ScalastylePlugin.autoImport._
import pl.project13.scala.sbt.JmhPlugin
import sbtassembly.AssemblyPlugin.autoImport._

/* Settings */
object FiloSettings {

  /* The REPL can’t cope with -Ywarn-unused:imports or -Xfatal-warnings
     so we disable for console */
  lazy val consoleSettings = Seq(
   scalacOptions in (Compile, console) ~= (_.filterNot(Set(
     "-Ywarn-unused-import",
     "-Xfatal-warnings"))),
   scalacOptions in (Test, console) ~= (_.filterNot(Set(
     "-Ywarn-unused-import",
     "-Xfatal-warnings"))))

  lazy val compilerSettings = Seq(
    autoAPIMappings := true,
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding", "UTF-8",
      "-unchecked",
      "-target:jvm-1.8",
      "-feature",
      "-Xfuture",
      "-Xfatal-warnings",
      "-Ywarn-inaccessible",
      "-Ywarn-dead-code",
      "-Ywarn-unused-import",
      "-Yno-adapted-args",
      "-language:existentials",
      "-language:experimental.macros",
      "-language:higherKinds",
      "-language:implicitConversions"
      // TODO relocate here: -Ywarn-unused-import, add -Ywarn-numeric-widen
      // TODO in 2.12: remove: -Yinline-warnings, add the new applicable ones

      // Don't enable this option. It makes all fields lazy, hurting performance.
      //"-Xcheckinit"
    ),

    javacOptions ++= Seq(
      "-encoding", "UTF-8"
    ))

  // Create a default Scala style task to run with tests
  lazy val testScalastyle = taskKey[Unit]("testScalastyle")

  lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")

  lazy val styleSettings = Seq(
    scalastyleFailOnError := true,
    testScalastyle := scalastyle.in(Test).toTask("").value,
    // (scalastyleConfig in Test) := "scalastyle-test-config.xml",
    // This is disabled for now, cannot get ScalaStyle to recognize the file above for some reason :/
    // (test in Test) <<= (test in Test) dependsOn testScalastyle,
    compileScalastyle := scalastyle.in(Compile).toTask("").value,
    // Is running this on compile too much?
    (compile in Test) := ((compile in Test) dependsOn compileScalastyle).value)

  lazy val evictionSettings = Seq(
    evictionWarningOptions in update := EvictionWarningOptions.default
      .withWarnTransitiveEvictions(false)
      .withWarnDirectEvictions(false)
      .withWarnScalaVersionEviction(false))

  // TODO disabled for now: "-Xlint:infer-any", "-Xlint",
  lazy val lintSettings = Seq(
    scalacOptions ++= Seq(
      "-Xlint:adapted-args",
      "-Xlint:nullary-unit",
      "-Xlint:inaccessible",
      "-Xlint:nullary-override",
      "-Xlint:missing-interpolator",
      "-Xlint:doc-detached",
      "-Xlint:private-shadow",
      "-Xlint:type-parameter-shadow",
      "-Xlint:poly-implicit-overload",
      "-Xlint:option-implicit",
      "-Xlint:delayedinit-select",
      "-Xlint:by-name-right-associative",
      "-Xlint:package-object-classes",
      "-Xlint:unsound-match",
      "-Xlint:stars-align"
    ),

    javacOptions ++= Seq(
      "-Xlint",
      "-Xlint:deprecation",
      "-Xlint:unchecked"
    ))

  lazy val disciplineSettings =
    compilerSettings ++
      lintSettings ++
      styleSettings ++
      evictionSettings ++
      consoleSettings

  lazy val testSettings = Seq(
    parallelExecution in Test := false,
    fork in Test := true,
    testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oF"),
    // Uncomment below to debug Typesafe Config file loading
    // javaOptions ++= List("-Xmx2G", "-Dconfig.trace=loads"),
    // Make Akka tests more resilient esp for CI/CD/Travis/etc.
    javaOptions ++= List("-Xmx4G", "-XX:+CMSClassUnloadingEnabled", "-Dakka.test.timefactor=3"),
    // Needed to avoid cryptic EOFException crashes in forked tests
    // in Travis with `sudo: false`.
    // See https://github.com/sbt/sbt/issues/653
    // and https://github.com/travis-ci/travis-ci/issues/3775
    concurrentRestrictions in Global := Seq(
      // Tags.limit(Tags.CPU, java.lang.Runtime.getRuntime().availableProcessors()),
      Tags.limit(Tags.CPU, 1),
      // limit to 1 concurrent test task, even across sub-projects
      Tags.limit(Tags.Test, 1),
      // Note: some components of tests seem to have the "Untagged" tag rather than "Test" tag.
      // So, we limit the sum of "Test", "Untagged" tags to 1 concurrent
      Tags.limitSum(1, Tags.Test, Tags.Untagged))
  )

  lazy val itSettings = Defaults.itSettings ++ Seq(
    fork in IntegrationTest := true,

    parallelExecution in IntegrationTest := false,

    internalDependencyClasspath in IntegrationTest := (Classpaths.concat(
      internalDependencyClasspath in IntegrationTest, exportedProducts in Test)).value)

  lazy val multiJvmSettings = SbtMultiJvm.multiJvmSettings ++ Seq(
    javaOptions in MultiJvm := Seq("-Xmx4G", "-Dakka.test.timefactor=3"),
    compile in MultiJvm := ((compile in MultiJvm) triggeredBy (compile in Test)).value)

  lazy val testMultiJvmToo = Seq(
    // make sure that MultiJvm tests are executed by the default test target,
    // and combine the results from ordinary test and multi-jvm tests
    executeTests in Test := {
      val testResults = (executeTests in Test).value
      val multiNodeResults = (executeTests in MultiJvm).value
      // passed, failed, error
      // 0, 1, 2
      val overall =
      // passed,
      (testResults.overall, multiNodeResults.overall) match {
        case (TestResult.Passed, TestResult.Failed | TestResult.Error) => multiNodeResults
        case (TestResult.Failed, TestResult.Error) => multiNodeResults
        case _ => testResults
      }
      Tests.Output(overall.overall,
        testResults.events ++ multiNodeResults.events,
        testResults.summaries ++ multiNodeResults.summaries)
    }
  )

  lazy val multiJvmMaybeSettings = multiJvmSettings ++ {
                                   if (sys.env.contains("MAYBE_MULTI_JVM")) testMultiJvmToo else Nil }

  // Fork a separate JVM for each test, instead of one for all tests in a module.
  // This is necessary for Spark tests due to initialization, for example
  lazy val jvmPerTestSettings = {
    def jvmPerTest(tests: Seq[TestDefinition]) =
      tests map { test =>
        Tests.Group(
          name = test.name,
          tests = Seq(test),
          runPolicy = Tests.SubProcess(ForkOptions()))
      }

    Seq(testGrouping in Test := ((definedTests in Test) map jvmPerTest).value)
  }

  // NOTE: The -Xms1g and using RemoteActorRefProvider (no Cluster startup) both help CLI startup times
  // Also note: CLI-specific config overrides are set in FilodbCluster.scala
  lazy val shellScript = """#!/bin/bash
  while [ "${1:0:2}" = "-D" ]
  do
    allprops="$allprops $1"
    shift
  done
  if [ ! -z "$JAVA_HOME" ]; then
    CMD="$JAVA_HOME/bin/java"
  else
    CMD="java"
  fi
  if [ ! -z "$FILO_CONFIG_FILE" ]; then
    config="-Dconfig.file=$FILO_CONFIG_FILE"
  fi
  : ${FILOLOG:="."}
  exec $CMD -Xmx4g -Xms1g -DLOG_DIR=$FILOLOG $config $allprops -jar "$0" "$@"  ;
  """.split("\n")

  lazy val kafkaSettings = Seq(

    aggregate in update := false,

    updateOptions := updateOptions.value.withCachedResolution(true))

  lazy val assemblySettings = Seq(
    assemblyMergeStrategy in assembly := {
      case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
      case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
      case m if m.toLowerCase.matches("meta-inf.*\\.properties") => MergeStrategy.discard
      case PathList(ps @ _*) if ps.last endsWith ".txt.1" => MergeStrategy.first
      case "reference.conf"    => MergeStrategy.concat
      case "application.conf"  => MergeStrategy.concat
      case "filodb-defaults.conf"  => MergeStrategy.concat
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },
    assemblyShadeRules in assembly := Seq(
      ShadeRule.rename("com.datastax.driver.**" -> "filodb.datastax.driver.@1").inAll,
      ShadeRule.rename("com.google.common.**" -> "filodb.com.google.common.@1").inAll,
      ShadeRule.rename("org.apache.http.**" -> "filodb.org.apache.http.@1").inAll,
      ShadeRule.rename("com.google.guava.**" -> "filodb.com.google.guava.@1").inAll
    ),
    test in assembly := {} //noisy for end-user since the jar is not available and user needs to build the project locally
  )

  lazy val assemblyExcludeScala = assemblySettings ++ Seq(
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false))

  // Builds cli as a standalone executable to make it easier to launch commands
  lazy val cliAssemblySettings = assemblySettings ++ Seq(
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(
      prependShellScript = Some(shellScript)),
    assemblyJarName in assembly := s"filo-cli-${version.value}"
  )

  // builds timeseries-gen as a fat jar so it can be executed for development test scenarios
  lazy val gatewayAssemblySettings = assemblySettings ++ Seq(
    assemblyJarName in assembly := s"gateway-${version.value}"
  )


  lazy val moduleSettings = Seq(

    cancelable in Global := true,

    scalaModuleInfo := scalaModuleInfo.value map {_.withOverrideScalaVersion(true)}
  )

  lazy val commonSettings =
      disciplineSettings ++
      moduleSettings ++
      testSettings
}
