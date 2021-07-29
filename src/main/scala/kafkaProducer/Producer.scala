package kafkaProducer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer


class Producer(topic: String, brokers: String) {

  val producer = new KafkaProducer[String, String](configuration)

  private def configuration: Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    props
  }

  def sendMessages(): Unit = {

    val messages: List[String] = List("message1", "message2", "message3")


    for (message <- messages) {
      producer.send(new ProducerRecord[String, String](topic, message))
    }
  }

}

object Producer {

  def main(args: Array[String]): Unit = {

    val producer = new Producer("test-topic", "localhost:9092")
    producer.sendMessages()
  }
}
