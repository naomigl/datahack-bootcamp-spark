package com.datahack.bootcamp.spark.exercises.twitter.utils

import java.net.URLEncoder

import org.json.JSONObject

import scala.util.Try

object GoogleMapsUtils {

  private val GOOGLE_URL = "http://maps.googleapis.com/maps/api/geocode/json?"
  private val QUERY_STRING = "address="
  private val SENSOR = "&sensor=false"

  def getLatLong(address: List[String]): Option[(Double, Double)] = {
    val url = GOOGLE_URL + QUERY_STRING + URLEncoder.encode(address.reduce(_ + "+" + _)) + SENSOR
    HTTPUtils.doGet(url).toOption.flatMap(parseResult)
  }

  private def parseResult(result: String): Option[(Double, Double)] = {
    Try {
      val obj = new JSONObject(result)
      val results = obj.getJSONArray("results").get(0).asInstanceOf[JSONObject]
      val location = results.getJSONObject("geometry").getJSONObject("location")
      val latitude = location.getDouble("lat")
      val longitude = location.getDouble("lng")
      (latitude, longitude)
    }.toOption
  }
}
