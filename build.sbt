import AssemblyKeys._

seq(assemblySettings: _*)

name := "ScalaReactiveExamples"

version := "v2014-01-20"

scalaVersion := "2.10.3"

scalacOptions ++= Seq("-unchecked", "-deprecation" )

mainClass in assembly := Some("dummy.Dummy")

jarName in assembly := "reactive.jar"

libraryDependencies ++= Seq(
   "com.netflix.rxjava" % "rxjava-scala" % "0.16.+",
   "fr.janalyse" %% "janalyse-ssh" % "0.9.12"
)

libraryDependencies += "org.scalatest" %% "scalatest" % "2.0.+" % "test"

libraryDependencies += "junit" % "junit" % "4.+" % "test"

resolvers += "JAnalyse Repository" at "http://www.janalyse.fr/repository/"

initialCommands in console := """
  import dummy._
  import fr.janalyse.ssh._
  import rx.lang.scala._
  import rx.lang.scala.schedulers._
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
