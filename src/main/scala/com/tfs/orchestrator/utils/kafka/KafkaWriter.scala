package com.tfs.orchestrator.utils.kafka

import java.util.{Properties}
import com.tfs.orchestrator.utils.{PropertyReader}
import com.tfs.orchestrator.utils.SlaRecordPublish.{OozieActionStats}
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.logging.log4j.scala.Logging

object KafkaWriter extends Logging {

  lazy val producer = getProducer

  def getProducer = {
    val props: Properties = new Properties();
    props.put("bootstrap.servers", PropertyReader.getApplicationProperties().getProperty("kafka.brokers"))
    props.put("retries", Integer.valueOf(1))
    props.put("linger.ms", Integer.valueOf(5000))
    new KafkaProducer[String,String](props,new StringSerializer(),new StringSerializer())
  }

  /**
   *
   * @param topicName
   * @param value
   */
  def pushMetrics(topicName: String, key: String, value: String): Unit = {
    val before = System.currentTimeMillis()

    /* Check for empty topic name */
    if (topicName.isEmpty || topicName.equals(null)) {
      logger.info("Not Pushing Dataplatform metrics to Kafka topic as topic name is not configured")
      return
    }
    logger.info("Writing Data Platform metrics into Kafka topic " + topicName)

    try {

      producer.send(new ProducerRecord(topicName, key, value))

      logger.info("Data Platform metrics pushed to kafka")
      logger.info(s"total time taken to push metrics= ${(System.currentTimeMillis() - before)} millis")
      closeProducer
    }
    catch {
      case ex: Exception => {
        logger.error("Exception while pushing dataplatform metrics to Kafka topic. ",ex)
        throw new RuntimeException("Unable to push to Kafka.")
      }
    }
  }

  def closeProducer: Unit = {
    producer.flush()
    producer.close()
  }

  case class PlatformDetailsStats(id: String, eventSource: String, eventTime: Long, body: PlatformStats)

  case class PlatformStats(viewName_keyword: String, clientName_keyword: String, jobSchedule_time: Long, dataStart_time: Long, dataEnd_time: Long,
                           actualStart_time: Long, actualEnd_time: Long, duration: Long, actions: List[OozieActionStats],
                           retry_count: Int, workflowStatus_keyword: String, dqStatus_keyword: String, parentId_keyword: String)

}
