package com.tfs.orchestrator.utils.kafka

import java.util
import java.util.Properties

import com.tfs.orchestrator.utils.PropertyReader
import org.apache.kafka.clients.consumer._
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.mockito.MockitoSugar

class KafkaReaderTests extends FunSuite with BeforeAndAfter with MockitoSugar {

  val propsDir = "src/main/resources/properties"
  PropertyReader.initialize(false, propsDir)

  private val kafkaConsumer = mock[KafkaConsumer[String, String]]
  private val consumerRecords = mock[ConsumerRecords[String, String]]
  private val msgIterator = mock[util.Iterator[ConsumerRecord[String, String]]]

  private val pollTimeout = 30000l

  test("kafka reader next for true.") {
    val kafkaReader = new KafkaReader {
      override def getKafkaConsumerInstance(props: Properties) = {
        kafkaConsumer
      }

      override def commit: Boolean = {
        true
      }

      override def getPollTimeout = {
        pollTimeout
      }

    }
    when(kafkaConsumer.poll(pollTimeout)).thenReturn(consumerRecords)
    when(consumerRecords.isEmpty).thenReturn(false)
    when(consumerRecords.iterator()).thenReturn(msgIterator)
    assert(kafkaReader.hasNext)
  }

  test("kafka reader next commit is false.") {
    val kafkaReader = new KafkaReader {

      override def getKafkaConsumerInstance(props: Properties) = {
        kafkaConsumer
      }

      override def commit: Boolean = {
        false
      }

      override def getPollTimeout = {
        pollTimeout
      }
    }
    assert(!kafkaReader.hasNext)

  }

  test("kafka reader next stopreading is true.") {
    val kafkaReader = new KafkaReader {

      override def getKafkaConsumerInstance(props: Properties) = {
        kafkaConsumer
      }

      override def commit: Boolean = {
        false
      }

      override def getPollTimeout = {
        pollTimeout
      }
    }
    kafkaReader.stopReading = true
    assert(!kafkaReader.hasNext)

  }

  test("kafka reader next record is empty.") {
    val kafkaReader = new KafkaReader {
      override def getKafkaConsumerInstance(props: Properties) = {
        kafkaConsumer
      }

      override def commit: Boolean = {
        true
      }

      override def getPollTimeout = {
        pollTimeout
      }

    }
    when(kafkaConsumer.poll(pollTimeout)).thenReturn(consumerRecords)
    when(consumerRecords.isEmpty).thenReturn(true)
    assert(!kafkaReader.hasNext)
  }


  test("commit successfully") {
    val kafkaReader = new KafkaReader {
      override def getKafkaConsumerInstance(props: Properties) = {
        kafkaConsumer
      }
    }
    doNothing().when(kafkaConsumer).commitSync()
    assert(kafkaReader.commit)

  }

  test("commit fails") {
    val kafkaReader = new KafkaReader {
      override def getKafkaConsumerInstance(props: Properties) = {
        kafkaConsumer
      }
    }
    when(kafkaConsumer.commitSync()).thenThrow(new CommitFailedException("Commit Failed"))
    assert(kafkaReader.commit)
    assert(kafkaReader.commit)
    assert(kafkaReader.commit)
    assert(!kafkaReader.commit)
  }


  test("close") {
    val kafkaReader = new KafkaReader {
      override def getKafkaConsumerInstance(props: Properties) = {
        kafkaConsumer
      }
    }
    doNothing().when(kafkaConsumer).unsubscribe()
    doNothing().when(kafkaConsumer).close()
    kafkaReader.close
  }
}
