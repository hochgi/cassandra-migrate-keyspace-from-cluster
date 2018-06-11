package hochgi.devops.cassandra.backup

import java.nio.ByteBuffer

import com.datastax.driver.core.{DataType, ProtocolVersion, TypeCodec}

class StringToDateCodec extends TypeCodec[String](DataType.date(), classOf[String]) {

  val delegate = TypeCodec.date()

  override def serialize(value: String, protocolVersion: ProtocolVersion): ByteBuffer = {
    val d = java.time.LocalDate.parse(value)
    val r = com.datastax.driver.core.LocalDate.fromYearMonthDay(d.getYear,d.getMonthValue,d.getDayOfMonth)
    delegate.serialize(r,protocolVersion)
  }

  override def deserialize(bytes: ByteBuffer, protocolVersion: ProtocolVersion): String = {
    delegate.deserialize(bytes,protocolVersion).toString
  }

  override def parse(value: String): String = value

  override def format(value: String): String = value
}
