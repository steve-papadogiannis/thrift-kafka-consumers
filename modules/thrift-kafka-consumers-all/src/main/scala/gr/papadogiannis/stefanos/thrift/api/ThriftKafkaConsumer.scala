package gr.papadogiannis.stefanos.thrift.api

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.{CommitterSettings, ConsumerMessage, ConsumerSettings, Subscriptions}
import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

abstract class ThriftKafkaConsumer[T <: ThriftStruct](codec: ThriftStructCodec[T], topic: String, bootstrapServers: String = "localhost:9092")
    extends App {

  implicit val system: ActorSystem = ActorSystem("ThriftKafkaConsumerActorSystem")

  val config = system.settings.config.getConfig("akka.kafka.consumer")

  val consumerSettings =
    ConsumerSettings(config, new StringDeserializer, new ThriftDeserializer(codec))
      .withBootstrapServers(bootstrapServers)
      .withGroupId("thrift-kafka-consumers-group-1")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val committerSettings = CommitterSettings(system)

  def extractEventType(t: T): String

  def filter(t: T): Boolean = true

  def handleMsg(msg: ConsumerMessage.CommittableMessage[String, T]) = {
    Future {
      if (filter(msg.record.value())) {
        println("\n********************************************************************************")
        println(s"Key: ${msg.record.key()}")
        println(s"EventType: ${extractEventType(msg.record.value())}")
        println(s"Msg: $msg")
      }
      Done
    }
  }

  val control: DrainingControl[Done] =
    Consumer
      .committableSource(consumerSettings, Subscriptions.topics(topic))
      .mapAsync(1) { msg =>
        handleMsg(msg)
          .map(_ => msg.committableOffset)
      }
      .toMat(Committer.sink(committerSettings))(DrainingControl.apply)
      .run()

  system.registerOnTermination {
    control.drainAndShutdown()
  }

}
