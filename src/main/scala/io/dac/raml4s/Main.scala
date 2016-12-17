package io.dac.raml4s


import org.raml.v2.api._
import scala.collection.JavaConverters._




/**
  * Created by dcollins on 12/6/16.
  */
object Main extends App {

  val api =
    new RamlModelBuilder()
        .buildApi(
          s"""
             |#%RAML 1.0
             |title: A CRUD API for Users and Groups
             |version: v1
             |mediaType: application/json
             |
             |types:
             |  Person:
             |  Another:
             |
             |/index/{key}:
             |  get:
             |    responses:
             |      200:
             |        body: Person[]
             |  post:
             |    body: Person
             |    responses:
             |      200:
             |        body: Person
             |
             |
           """.stripMargin, "example")

  if (api.hasErrors) {
    api.getValidationResults.asScala.foreach(println)
  } else {
    val it = api.getApiV10
    it.resources.asScala.foreach(println)

    val resourceMap = it.resources.asScala.map(r => r.resourcePath -> r).toMap
    val r = it.resources.asScala(0)

    r.parentResource()
  }

}
