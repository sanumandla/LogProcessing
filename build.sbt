import AssemblyKeys._

name := "logprocessing"

version := "0.1-SNAPSHOT"

autoScalaLibrary := false

resolvers += "Concurrent Maven Repo" at "http://conjars.org/repo"

resolvers += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

resolvers += "A1PublishRepo" at "http://a1repo.agilone.com/artifactory/a1repo"

credentials += Credentials("Artifactory Realm", "a1repo.agilone.com", "build-master", "{DESede}v4RJ9Xfh7F2wth93CWWWUA==")

libraryDependencies ++= Seq(
		"cascading" % "cascading-core" % "2.1.3",
    	"cascading" % "cascading-local" % "2.1.3",
    	"cascading" % "cascading-hadoop" % "2.1.3",
    	"cascading" % "cascading-test" % "2.0.8",
    	"org.apache.hadoop" % "hadoop-test" % "1.0.3",
        "ch.qos.logback" % "logback-classic" % "1.0.11"
)

seq(assemblySettings: _*)

mainClass := Some("LogAnalyzer")

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case _ => MergeStrategy.last
  }
}
