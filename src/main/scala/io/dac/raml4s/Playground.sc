import scala.collection.JavaConverters._
import org.raml.v2.api._
import org.raml.v2.api.model.v10._
import org.raml.v2.api.model.v10.methods.Method

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
         |  description: Something
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
  val r = it.resources.get(0)

  r.description.value
  r.methods.asScala.map{ (m: Method) => m.method() -> m.resource.resourcePath}

}