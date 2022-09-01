package gr.papadogiannis.stefanos.thrift.api

import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec}
import org.apache.kafka.common.serialization.Deserializer

import java.util

final class ThriftDeserializer[V <: ThriftStruct](codec: ThriftStructCodec[V]) extends Deserializer[V] {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = { }

  override def close(): Unit = { }

  def deserialize(topic: String, data: Array[Byte]): V = ThriftDecoders.ByteArray(codec, data)

}