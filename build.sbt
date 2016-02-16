name := "nlptab"

organization := "edu.umn.nlptab"

version := "2.0-SNAPSHOT"

lazy val `nlptab` = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.7"

pipelineStages := Seq(uglify, digest, gzip)

pipelineStages in Assets := Seq()

pipelineStages := Seq(uglify, digest, gzip)

DigestKeys.algorithms += "sha1"

UglifyKeys.uglifyOps := { js =>
  Seq((js.sortBy(_._2), "concat.min.js"))
}

libraryDependencies ++= Seq(
  "com.google.inject" % "guice" % "4.0",
  "javax.inject" % "javax.inject" % "1",
  "org.elasticsearch" % "elasticsearch" % "2.2.0",
  "com.typesafe.play" %% "play-slick" % "1.1.1",
  "org.webjars" % "bootstrap" % "4.0.0-alpha.2",
  "org.webjars" % "angularjs" % "1.5.0",
  "org.webjars" % "angular-ui-bootstrap" % "1.1.1-1"
)

unmanagedResourceDirectories in Test <+=  baseDirectory ( _ /"target/web/public/test" )  

resolvers += "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases"
