package com.datahack.bootcamp.spark.exercises.twitter.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.dayofmonth
import org.apache.spark.sql.functions.month
import org.apache.spark.sql.functions.year
import org.apache.spark.sql.types.TimestampType
import org.neo4j.spark.Neo4jConfig
import org.neo4j.spark.Neo4jDataFrame.execute


class WriterFunctions(self: DataFrame, session: SparkSession) {

  val TIME = "time"

  def writeDF(format: String, cnf: Map[String, String], partitionEnabled: Boolean = false): Unit = {
    if(!self.rdd.isEmpty) {
      val writer = self.write.format(format).options(cnf).mode(SaveMode.Append)
      if (partitionEnabled) {
        self
          .withColumn("year", year(self.col(TIME).cast(TimestampType)))
          .withColumn("month", month(self.col(TIME).cast(TimestampType)))
          .withColumn("day", dayofmonth(self.col(TIME).cast(TimestampType)))
          .write.format(format).options(cnf).mode(SaveMode.Append).partitionBy("year", "month", "day").save()
      }
      else writer.save()
    }
  }

  def writeNeo4jDF: Unit = {
    val mergeStatement = s"""
      UNWIND {rows} as row
      MERGE (source:`User` {`id` : row.source.`id`, `screen_name` : row.source.`screen_name`, `name` : row.source.`name`}) ON CREATE SET source+=row.source
      MERGE (target:`User` {`id` : row.target.`id`, `screen_name` : row.target.`screen_name`, `name` : row.target.`name`}) ON CREATE SET target+=row.target
      MERGE (source)-[rel:`RT` {`tweet_id` : row.relationship.`tweet_id`, `sentiment`: row.relationship.`sentiment` }]->(target) ON CREATE SET rel+=row.relationship
      """
    val partitions = Math.max(1,(self.count() / 10000).asInstanceOf[Int])
    val config = Neo4jConfig(session.sparkContext.getConf)
    import scala.collection.JavaConverters._
    self.repartition(partitions).foreachPartition( rows => {
      val params: AnyRef = rows.map(r =>
        Map(
          "source" -> Map(
            "id" -> r.getAs[AnyRef]("authorId"),
            "name" -> r.getAs[AnyRef]("authorName"),
            "screen_name" -> r.getAs[AnyRef]("authorScreenName")
          ).asJava,
          "target" -> Map(
            "id" -> r.getAs[AnyRef]("rtById"),
            "name" -> r.getAs[AnyRef]("rtByName"),
            "screen_name" -> r.getAs[AnyRef]("rtByScreenName")
          ).asJava,
          "relationship" -> Map(
            "tweet_id" -> r.getAs[AnyRef]("tweetId"),
            "sentiment"-> r.getAs[AnyRef]("sentiment")).asJava)
          .asJava).asJava
      println(params)
      execute(config, mergeStatement, Map("rows" -> params).asJava)
    })
  }

}

trait GenericWriter {
  implicit def writerFunctions(dataFrame: DataFrame)(implicit session: SparkSession): WriterFunctions =
    new WriterFunctions(dataFrame, session)
}

object GenericWriter extends GenericWriter
