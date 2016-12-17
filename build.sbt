name := "merkle-index"

version := "1.0"

scalaVersion := "2.12.1"


libraryDependencies ++= Vector(
  // Scala Runtime Dependencies
  "org.typelevel"         %% "cats"                 % "0.8.1",

  // Java Dependencies
  "org.raml"              %  "raml-parser-2"        % "1.0.3"
) ++ Vector(
  // Scala Test Dependencies
  "org.scalactic"         %% "scalactic"            % "3.0.1",
  "org.scalatest"         %% "scalatest"            % "3.0.1"
).map(_ % "test")

