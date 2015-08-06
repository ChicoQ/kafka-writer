package com.td.kafka

import java.util.Properties
import kafka.producer.{ProducerConfig, Producer, KeyedMessage}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

/**
 * Created by Chico on 12/10/14.
 * firstly, setting kafka producer in producer-defaults.properties
 * sample code:
 * rdd.write2Kafka((x: String) => new KeyedMessage[String, String](topic, null, x))
 */

object KafkaWriter {
  import scala.language.implicitConversions

  implicit def createKafkaWriter[T: ClassTag](rdd: RDD[T]): KafkaWriter[T] = {
    new KafkaWriter[T](rdd)
  }
}

/*
 * kafka Producer setting is inside resources/producer-defaults.properties
 * Producer and producer.send are running on rdd.foreachPartition
 * serializerFunc
 */

class KafkaWriter[T: ClassTag](@transient rdd: RDD[T]) extends Serializable with Logging {

  def write2Kafka[K, V](serializerFunc: T => KeyedMessage[K, V]) {

    val props = new Properties()
    props.load(this.getClass.getResourceAsStream("/producer-defaults.properties"))
    val broadcastedConfig = rdd.sparkContext.broadcast(props)

    rdd.foreachPartition(events => {
      val producer: Producer[K, V] = new Producer[K, V](new ProducerConfig(broadcastedConfig.value))
      try {
        producer.send(events.map(serializerFunc).toArray: _*)
        logDebug("Data sent successfully to Kafka")
      } catch {
        case e: Exception =>
          logError("Failed to send data to Kafka", e)
          throw e
      } finally {
        producer.close()
        logDebug("Kafka Producer closed successfully")
      }
    })
  }
}


