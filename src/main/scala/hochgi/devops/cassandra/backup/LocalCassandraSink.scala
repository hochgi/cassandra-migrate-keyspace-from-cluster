package hochgi.devops.cassandra.backup

import akka.Done
import akka.stream.alpakka.cassandra.scaladsl.CassandraSink
import akka.stream.scaladsl.Sink
import com.datastax.driver.core.{DataType, PreparedStatement, Row, Session}
import com.typesafe.config.Config

import scala.concurrent.Future

object LocalCassandraSink {

  def cassandraSink(conf: Config, session: Session): Sink[Row, Future[Done]] = {
    val preparedStatement = session.prepare(conf.getString("hochgi.devops.cassandra.local.insert-statement"))
    val statementBinder = (row: Row, stmt: PreparedStatement) => {
      var bs = stmt.bind()
      row.getColumnDefinitions
        .iterator()
        .forEachRemaining { coldef =>
          val colName = coldef.getName
          bs = coldef.getType.getName match {
            case DataType.Name.TEXT | DataType.Name.VARCHAR => bs.setString(colName, row.getString(colName))
            case DataType.Name.ASCII     => bs.setString(colName,    row.getString(colName))
            case DataType.Name.BIGINT    => bs.setLong(colName,      row.getLong(colName))
            case DataType.Name.BOOLEAN   => bs.setBool(colName,      row.getBool(colName))
            case DataType.Name.DECIMAL   => bs.setDecimal(colName,   row.getDecimal(colName))
            case DataType.Name.DOUBLE    => bs.setDouble(colName,    row.getDouble(colName))
            case DataType.Name.FLOAT     => bs.setFloat(colName,     row.getFloat(colName))
            case DataType.Name.INT       => bs.setInt(colName,       row.getInt(colName))
            case DataType.Name.TIMESTAMP => bs.setTimestamp(colName, row.getTimestamp(colName))
            case DataType.Name.UUID      => bs.setUUID(colName,      row.getUUID(colName))
            case DataType.Name.VARINT    => bs.setVarint(colName,    row.getVarint(colName))
            case DataType.Name.INET      => bs.setInet(colName,      row.getInet(colName))
            case DataType.Name.DATE      => bs.setDate(colName,      row.getDate(colName))
            case DataType.Name.TIME      => bs.setTime(colName,      row.getTime(colName))
            case DataType.Name.CUSTOM    => ???
            case DataType.Name.BLOB      => ???
            case DataType.Name.COUNTER   => ???
            case DataType.Name.TIMEUUID  => ???
            case DataType.Name.SMALLINT  => bs.setShort(colName,     row.getShort(colName))
            case DataType.Name.TINYINT   => bs.setByte(colName,      row.getByte(colName))
            case DataType.Name.DURATION  => ???
            case DataType.Name.LIST      => ???
            case DataType.Name.MAP       => ???
            case DataType.Name.SET       => ???
            case DataType.Name.UDT       => ???
            case DataType.Name.TUPLE     => ???
          }
        }
      CassandraMirrorBackupper.ingestedRowRate.mark()
      bs
    }

    CassandraSink[Row](conf.getInt("hochgi.devops.cassandra.local.parallelism"), preparedStatement, statementBinder)(session)
  }
}
