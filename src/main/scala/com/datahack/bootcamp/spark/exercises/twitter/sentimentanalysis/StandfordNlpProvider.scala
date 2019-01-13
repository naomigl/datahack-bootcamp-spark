package com.datahack.bootcamp.spark.exercises.twitter.sentimentanalysis

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation
import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.util.CoreMap

import scala.collection.JavaConverters._
import scala.util.Try

object StandfordNlpProvider {

  val props = new Properties()
  props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref, sentiment")
  val pipeline = new StanfordCoreNLP(props)

  def extractEntities(text: String): Try[Seq[(String, String, Int, Int)]] = {
    Try {
      val sent = new Annotation(text)
      pipeline.annotate(sent)
      sent.get(classOf[SentencesAnnotation])
        .get(0)
        .get(classOf[TokensAnnotation]).asScala
        .map { corelabel =>
          (corelabel.get(classOf[TextAnnotation]), corelabel.get(classOf[NamedEntityTagAnnotation]), corelabel
            .beginPosition(), corelabel.endPosition())
        }.filter(_._2 != "O").toList
    }
  }

  def extractSentiment(text: String): Try[String] = {
    Try {
      val annotation = new Annotation(text)
      pipeline.annotate(annotation)

      val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
      val sentence: CoreMap = sentences.get(0)
      sentence.get(classOf[SentimentCoreAnnotations.SentimentClass])
    }
  }

}

