package gr.papadogiannis.stefanos.thrift.api

import com.twitter.scrooge.{ThriftStruct, ThriftStructCodec}
import org.apache.thrift.protocol.{TCompactProtocol, TProtocol}
import org.apache.thrift.transport.TTransport

trait ThriftDecoder {

  type Input
  type Transport <: TTransport
  type Protocol <: TProtocol

  def createTransport(input: Input): Transport

  def createProtocol[T <: ThriftStruct](codec: ThriftStructCodec[T], transport: Transport): Protocol

  def apply[T <: ThriftStruct](codec: ThriftStructCodec[T], input: Input): T
}

trait DefaultThriftDecoder extends ThriftDecoder {

  type Protocol = TProtocol

  override def createProtocol[T <: ThriftStruct](codec: ThriftStructCodec[T], transport: Transport): Protocol = {
    (new TCompactProtocol.Factory).getProtocol(transport)
  }

  override def apply[T <: ThriftStruct](codec: ThriftStructCodec[T], input: Input): T = {
    val transport = createTransport(input)
    val protocol = createProtocol(codec, transport)
    codec.decode(protocol)
  }
}