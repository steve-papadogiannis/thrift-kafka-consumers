package gr.papadogiannis.stefanos.thrift.api

import org.apache.thrift.transport.TMemoryInputTransport

object ThriftDecoders {

  object ByteArray extends DefaultThriftDecoder {
    type Input = Array[Byte]
    type Transport = TMemoryInputTransport

    def createTransport(input: Input): Transport = new TMemoryInputTransport(input)
  }

}
