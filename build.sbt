name := "ts-index"

version := "1.0"

scalaVersion := "2.12.1"


libraryDependencies ++= Vector(
  // Scala Runtime Dependencies
  "org.typelevel"               %% "cats"                 % "0.8.1",
  "com.typesafe.scala-logging"  %% "scala-logging"        % "3.5.0",

  // Java Dependencies
  "org.raml"              %  "raml-parser-2"        % "1.0.3",
  "ch.qos.logback"        % "logback-classic"       % "1.1.7"
) ++ Vector(
  // Scala Test Dependencies
  "org.scalactic"         %% "scalactic"            % "3.0.1",
  "org.scalatest"         %% "scalatest"            % "3.0.1",
  "org.scalacheck"        %% "scalacheck"           % "1.13.4"
).map(_ % "test")

