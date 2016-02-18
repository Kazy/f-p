// `scalacOptions` for sbt itself
scalacOptions += "-deprecation"

libraryDependencies += "com.lihaoyi" % "ammonite-repl" % "0.5.4" % "test" cross CrossVersion.full

initialCommands in (Test, console) := """ammonite.repl.Main.run("")"""
