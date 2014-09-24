package com.pongr.metrics.amazoncloudwatch


import java.util.Date
import java.util.concurrent.TimeUnit
import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.collection.JavaConversions._

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient
import com.amazonaws.services.cloudwatch.model.{ StandardUnit, PutMetricDataRequest, MetricDatum, Dimension, StatisticSet }

case class AmazonCloudWatch(
  awsCredentialProvider: AWSCredentialsProvider,
  awsCloudWatchEndPoint: String,
  nameSpace: String,
  dimensions: Map[String, String],
  smallestValueToConsider: Double = 0.0001
) extends LazyLogging {

  val dims     = dimensions.map { case (k, v) => (new Dimension).withName(k).withValue(v) }
  val client   = new AmazonCloudWatchClient(awsCredentialProvider)
  client.setEndpoint(awsCloudWatchEndPoint)


  def timestamp = System.currentTimeMillis

  def toStandardUnit(timeUnit: Option[TimeUnit]): StandardUnit = timeUnit.map(toStandardUnit) getOrElse StandardUnit.None

  def toStandardUnit(timeUnit: TimeUnit): StandardUnit = timeUnit match {
    case TimeUnit.NANOSECONDS  => StandardUnit.Microseconds
    case TimeUnit.MILLISECONDS => StandardUnit.Milliseconds
    case TimeUnit.SECONDS      => StandardUnit.Seconds
    case TimeUnit.MINUTES      => StandardUnit.None
    case TimeUnit.HOURS        => StandardUnit.None
    case TimeUnit.DAYS         => StandardUnit.None
    case _                     =>
      logger.warn("do not know how to convert timeunit [{}] to StandardUnit", timeUnit)
      StandardUnit.None
  }

  def toRateUnit(timeUnit: TimeUnit): StandardUnit = timeUnit match {
      case TimeUnit.SECONDS => StandardUnit.CountSecond
      case _ =>
        logger.warn("do not know how to convert rate timeunit [{}] to StandardUnit", timeUnit)
        StandardUnit.None
    }

  def sendStats(name: String, stats: StatisticSet, timestamp: Long, timeUnit: Option[TimeUnit] = None) {
    if (stats.getSum != 0 && stats.getSampleCount != 0) {
      val metricData = (new MetricDatum()).withDimensions(dims)
                                          .withMetricName(name)
                                          .withStatisticValues(stats)
                                          .withTimestamp(new Date(timestamp))
                                          .withUnit(toStandardUnit(timeUnit))
      send(metricData)
    }
    else
      logger.debug("{} value is 0, cannot be sent to CloudWatch.", name)
  }

  private def normalizeCloudWatchValue(value: Double): Double = {
    if (value > -1 * smallestValueToConsider && value < smallestValueToConsider) // cloudwatch doesn't like very small values, this is small enough for us to be negligible
      0
    else
      value
  }

  def sendValueWithUnit(name: String, value: Double, timestamp: Long, unit: StandardUnit) {
    val metricData = (new MetricDatum()).withDimensions(dims)
      .withMetricName(name)
      .withValue(normalizeCloudWatchValue(value))
      .withTimestamp(new Date(timestamp))
      .withUnit(unit)

    send(metricData)
  }

  def sendValue(name: String, value: Double, timestamp: Long, timeUnit: Option[TimeUnit] = None) {
    sendValueWithUnit(name, value, timestamp, toStandardUnit(timeUnit))
  }

  def send(metricData: MetricDatum) {
    val metricRequest = (new PutMetricDataRequest).withMetricData(metricData).withNamespace(nameSpace)
    try {
      client.putMetricData(metricRequest)
    }
    catch {
      case e: Exception =>
        logger.warn(s"failed to put metrics [$metricRequest] on cloudwatch", e)
    }
  }

}
