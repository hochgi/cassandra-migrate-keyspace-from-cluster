package hochgi.devops.cassandra.backup

import java.nio.ByteBuffer

import com.datastax.driver.core.exceptions.InvalidTypeException
import com.datastax.driver.core.{DataType, ProtocolVersion, TypeCodec}

class IntToBigintCodec extends TypeCodec[Integer](DataType.bigint(), classOf[Integer]) {

  val delegate = TypeCodec.bigint()

  override def serialize(value: Integer, protocolVersion: ProtocolVersion): ByteBuffer =
    delegate.serialize(value.longValue(),protocolVersion)

  override def deserialize(bytes: ByteBuffer, protocolVersion: ProtocolVersion): Integer = {
    val l = delegate.deserialize(bytes, protocolVersion)
    if(l <= Int.MaxValue && l >= Int.MinValue) l.intValue()
    else throw new InvalidTypeException("Long value does'nt fit in an Integer")
  }

  override def parse(value: String): Integer = Integer.parseInt(value)

  override def format(value: Integer): String = value.toString
}
