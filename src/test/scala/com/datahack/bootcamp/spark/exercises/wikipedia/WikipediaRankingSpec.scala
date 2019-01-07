package com.datahack.bootcamp.spark.exercises.wikipedia

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class WikipediaRankingSpec
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {

  // Este método inicializa la clase WikipediaRanking para tener un spark contest inicializado para los test.
  def initializeWikipediaRanking(): Boolean =
    try {
      WikipediaRanking
      true
    } catch {
      case ex: Throwable =>
        println(ex.getMessage)
        ex.printStackTrace()
        false
    }

  "Wikipedia Ranking" should "occurrencesOfLang method should work for (specific) RDD with one element" in {
    initializeWikipediaRanking()
    import WikipediaRanking._
    val rdd = sc.parallelize(Seq(WikipediaArticle("title", "Java Jakarta")))
    occurrencesOfLang("Java", rdd) shouldBe 1
  }

  "Wikipedia Ranking" should "rankLangs method should work for RDD with two elements" in {
    initializeWikipediaRanking()
    import WikipediaRanking._
    val langs = List("Scala", "Java")
    val rdd = sc.parallelize(List(
      WikipediaArticle("1", "Scala is great"),
      WikipediaArticle("2", "Java is OK, but Scala is cooler")
    ))
    val ranked = rankLangs(langs, rdd)
    ranked.head._1 shouldBe "Scala"
  }

  "Wikipedia Ranking" should "makeIndex method creates a simple index with two entries" in {
    import WikipediaRanking._
    val langs = List("Scala", "Java")
    val articles = List(
      WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
      WikipediaArticle("2","Scala and Java run on the JVM"),
      WikipediaArticle("3","Scala is not purely functional")
    )
    val rdd = sc.parallelize(articles)
    val index = makeIndex(langs, rdd)
    index.count() shouldBe 2
  }

  "Wikipedia Ranking" should "rankLangsUsingIndex method should work for a simple RDD with three elements" in {
    import WikipediaRanking._
    val langs = List("Scala", "Java")
    val articles = List(
      WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
      WikipediaArticle("2","Scala and Java run on the JVM"),
      WikipediaArticle("3","Scala is not purely functional")
    )
    val rdd = sc.parallelize(articles)
    val index = makeIndex(langs, rdd)
    val ranked = rankLangsUsingIndex(index)
    ranked.head._1 shouldBe "Scala"
  }

  // Descomenta este test cuando hayas implementado el método rankLangsReduceByKey para probarlo
  "Wikipedia Ranking" should "rankLangsReduceByKey method should work for a simple RDD with four elements" in {
    import WikipediaRanking._
    val langs = List("Scala", "Java", "Groovy", "Haskell", "Erlang")
    val articles = List(
      WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
      WikipediaArticle("2","Scala and Java run on the JVM"),
      WikipediaArticle("3","Scala is not purely functional"),
      WikipediaArticle("4","The cool kids like Haskell more than Java"),
      WikipediaArticle("5","Java is for enterprise developers")
    )
    val rdd = sc.parallelize(articles)
    val ranked = rankLangsReduceByKey(langs, rdd)
    ranked.head._1 shouldBe "Java"
  }
}
