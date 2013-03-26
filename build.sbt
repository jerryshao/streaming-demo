name := "streaming-demo"

version := "0.0.1"

scalaVersion := "2.9.2"

libraryDependencies += "org.spark-project" %% "spark-core" % "0.7.0"

libraryDependencies += "org.spark-project" %% "spark-streaming" % "0.7.0"

resolvers ++= Seq(
   "Maven Repository" at "http://repo1.maven.org/maven2",
   "Akka Repository" at "http://repo.akka.io/releases/",
   "Spray Repository" at "http://repo.spray.cc/"
)

unmanagedBase <<= baseDirectory { base => base / "lib" }
