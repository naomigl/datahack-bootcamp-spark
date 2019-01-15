package com.datahack.bootcamp.spark.exercises.wikipedia

import java.io.File

object WikipediaData {

  def filePath = {
    //val resource = this.getClass.getClassLoader.getResource("src/main/resources/wikipedia.dat")
    //new File(resource.toURI).getPath
    "src/main/resources/wikipedia.dat"
  }

  def parse(line: String): WikipediaArticle = {
    val subs = "</title><text>"
    val i = line.indexOf(subs)
    val title = line.substring(14, i)
    val text  = line.substring(i + subs.length, line.length-16)
    WikipediaArticle(title, text)
  }
}
