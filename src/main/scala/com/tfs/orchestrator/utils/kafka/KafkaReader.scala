package com.tfs.orchestrator.utils.kafka

import java.util
import java.util.Properties

import com.tfs.orchestrator.utils.PropertyReader
import org.apache.commons.io.IOUtils
import org.apache.kafka.clients.consumer._
import org.apache.logging.log4j.scala.Logging


class KafkaReader extends Logging with util.Iterator[String] {

  protected var msgIterator: util.Iterator[ConsumerRecord[String, String]] = null
  private var kafkaConsumer: KafkaConsumer[String, String] = null
  private var remainingRetryCount: Int = 3
  private val pollTimeout = getPollTimeout

  var stopReading = false

  initialize

  private def initialize = {

    val brokers = PropertyReader.getApplicationProperties().getProperty("kafka.brokers","localhost:9092")
    val consumerId = PropertyReader.getApplicationProperties().getProperty("kafka.consumerid","dp2exporter")
    val topic = PropertyReader.getApplicationProperties().getProperty("kafka.topic")


    val sessionTimeOut = Integer.valueOf(PropertyReader.getApplicationProperties().getProperty("kafka.session.timeout","300000"))
    val requestTimeOut = Integer.valueOf(PropertyReader.getApplicationProperties().getProperty("kafka.request.timeout","300005"))
    val maxPollInterval = Integer.valueOf(PropertyReader.getApplicationProperties().getProperty("kafka.max.poll.interval","300005"))

    val props = new Properties
    props.put("bootstrap.servers", brokers)
    props.put("group.id", consumerId)
    props.put("enable.auto.commit", "false")
    props.put("session.timeout.ms", sessionTimeOut)
    props.put("request.timeout.ms", requestTimeOut)
    props.put("max.poll.records", Integer.valueOf(PropertyReader.getApplicationProperties().getProperty("kafka.poll.count","50")))
    props.put("max.poll.interval.ms", maxPollInterval)
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "earliest")
    kafkaConsumer = getKafkaConsumerInstance(props)
    kafkaConsumer.subscribe(java.util.Arrays.asList(topic))

    logger.info("Subscribed to topic:" + topic)
  }


  protected def getKafkaConsumerInstance(props: Properties) :KafkaConsumer[String,String] = {
    new KafkaConsumer[String, String](props)
  }

  override def hasNext: Boolean = {

    if (msgIterator != null && msgIterator.hasNext) return msgIterator.hasNext

    msgIterator = null

    if (false == commit) {
      return false
    }
    if (stopReading)
      return false

    val records = kafkaConsumer.poll(pollTimeout)

    if (records.isEmpty) {
      logger.info("No more records to read")
      return false
    }

    logger.info("Read " + records.count + " messages from kafka")

    msgIterator = records.iterator

    return true
  }

  override def next(): String = {
    msgIterator.next().value()
  }

  def commit: Boolean = {
    logger.info("Committing offset to kafka")
    try
      kafkaConsumer.commitSync()
    catch {
      case e@(_: CommitFailedException | _: RetriableCommitFailedException) =>
        if (remainingRetryCount > 0) {
          logger.warn("Commit failed, continuing with processing", e)
          remainingRetryCount -= 1
        }
        else {
          logger.error("Commit failed, failing !!!", e)
          return false
        }
    }
    true
  }

  def close: Unit = {
    kafkaConsumer.unsubscribe()
    IOUtils.closeQuietly(kafkaConsumer)
  }

  protected def getPollTimeout = {
    PropertyReader.getApplicationProperties().getProperty("kafka.poll.timeout", "300000").toLong
  }
}

object KafkaReader extends Logging {
  def apply(): KafkaReader = {
    logger.info("Called KafkaReader")
    new KafkaReader
  }
}
