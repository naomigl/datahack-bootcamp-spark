package com.datahack.bootcamp.spark.exercises.twitter.sentimentanalysis.dandelion.model

import play.api.libs.json.Json
import play.api.libs.json.Reads
import play.api.libs.json.Writes

case class EntitiesAnalysisResult(
                                   time: Int,
                                   annotations: List[Annotation],
                                   lang: String,
                                   langConfidence: Float,
                                   timestamp: String
                                 ) extends Serializable

object EntitiesAnalysisResult extends Serializable {

  implicit val writer: Writes[EntitiesAnalysisResult] = Json.writes[EntitiesAnalysisResult]
  implicit val reader: Reads[EntitiesAnalysisResult] = Json.reads[EntitiesAnalysisResult]

}
