name := "merkle-index"

version := "1.0"

scalaVersion := "2.12.0"

addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.3")

libraryDependencies ++= Vector(
  "org.typelevel"         %% "cats"                 % "0.8.1"
) ++ Vector(
  "org.scalactic"         %% "scalactic"            % "3.0.1",
  "org.scalatest"         %% "scalatest"            % "3.0.1"
).map(_ % "test")
