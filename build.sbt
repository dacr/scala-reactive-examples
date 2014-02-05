import AssemblyKeys._

seq(assemblySettings: _*)

name := "ScalaReactiveExamples"

version := "v2014-01-20"

scalaVersion := "2.10.3"

scalacOptions ++= Seq("-unchecked", "-deprecation" )

mainClass in assembly := Some("dummy.Dummy")

jarName in assembly := "reactive.jar"

libraryDependencies ++= Seq(
   "com.github.scala-incubator.io" %% "scala-io-core"      % "0.4.2",
   "com.github.scala-incubator.io" %% "scala-io-file"      % "0.4.2",
   "net.databinder.dispatch" %% "dispatch-core" % "0.11.+",
   "com.netflix.rxjava" % "rxjava-scala" % "0.16.+",
   "org.reactivemongo" %% "reactivemongo" % "0.10.0",
   "fr.janalyse" %% "janalyse-ssh" % "0.9.12",
   "fr.janalyse" %% "primes" % "1.0.4"
)

libraryDependencies += "org.scalatest" %% "scalatest" % "1.9.+" % "test"

libraryDependencies += "junit" % "junit" % "4.+" % "test"

resolvers += "JAnalyse Repository" at "http://www.janalyse.fr/repository/"

resolvers += "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases/"

initialCommands in console := """
  import dummy._
  import fr.janalyse.ssh._
  import rx.lang.scala._
  import rx.lang.scala.schedulers._
  import reactivemongo.api._
  import dispatch._
  //import Defaults._
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent._
  import scala.concurrent.duration._
"""

sourceGenerators in Compile <+= 
 (sourceManaged in Compile, version, name, jarName in assembly) map {
  (dir, version, projectname, jarexe) =>
  val file = dir / "dummy" / "MetaInfo.scala"
  IO.write(file,
  """package dummy
    |object MetaInfo { 
    |  val version="%s"
    |  val project="%s"
    |  val jarbasename="%s"
    |}
    |""".stripMargin.format(version, projectname, jarexe.split("[.]").head) )
  Seq(file)
}
