package com.datahack.bootcamp.spark.exercises.twitter.sentimentanalysis.dandelion.model

import play.api.libs.json.Json
import play.api.libs.json.Reads
import play.api.libs.json.Writes

case class SentimentAnalysisResult(
                                    time: Int,
                                    sentiment: Sentiment,
                                    lang: String,
                                    langConfidence: Float,
                                    timestamp: String
                                  ) extends Serializable

object SentimentAnalysisResult extends Serializable {

  implicit val writer: Writes[SentimentAnalysisResult] = Json.writes[SentimentAnalysisResult]
  implicit val reader: Reads[SentimentAnalysisResult] = Json.reads[SentimentAnalysisResult]

}
