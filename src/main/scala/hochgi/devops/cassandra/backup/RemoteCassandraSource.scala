package hochgi.devops.cassandra.backup

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.datastax.driver.core.{Row, Session}
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.collection.{breakOut, mutable}

class RemoteCassandraSource(conf: Config) {

  val parallelism: Int = conf.getInt("hochgi.devops.cassandra.remote.parallelism")
  val stmt: String = conf.getString("hochgi.devops.cassandra.remote.select-statement")
  val numberOfKeysToBind: Int = conf.getInt("hochgi.devops.cassandra.remote.key-arrity")

  require(numberOfKeysToBind > 0, s"No keys to bind select message can be used since defined key-arrity is [$numberOfKeysToBind]")

  val listOfKeysToBind: List[List[AnyRef]] = {
    if (numberOfKeysToBind == 1) conf.getStringList(s"hochgi.devops.cassandra.remote.key-1").asScala.map(List.apply(_))(breakOut[mutable.Buffer[String], List[AnyRef], List[List[AnyRef]]])
    else (for (i <- 1 to numberOfKeysToBind) yield conf.getAnyRefList(s"hochgi.devops.cassandra.remote.key-$i").asScala.view.map {
      case r: AnyRef  => r
      case d: Double  => Double.box(d)
      case f: Float   => Float.box(f)
      case l: Long    => Long.box(l)
      case i: Int     => Int.box(i)
      case c: Char    => Char.box(c)
      case s: Short   => Short.box(s)
      case b: Byte    => Byte.box(b)
      case b: Boolean => Boolean.box(b)
    }.toList).toList
  }

  println(listOfKeysToBind.map(_.mkString("[", ",", "]")).mkString("Running with partition keys lists: [\n\t", ",\n\t", "\n]"))

  def cassandraSource(session: Session): Source[Row,NotUsed] = {

    val myCodecRegistry = session.getCluster.getConfiguration.getCodecRegistry
    myCodecRegistry.register(new StringToDateCodec, new IntToBigintCodec)

    val preparedStatement = session.prepare(stmt)

    Source
      .fromIterator(() => Util.cartesianProduct(listOfKeysToBind))
      .flatMapMerge(parallelism, { keysToBind =>
        println(s"Starting Cassandra source for [$stmt] with bindings ${keysToBind.mkString("[", ",", "]")}")
        // original implementation with alpakka's Source:
        //          CassandraSource(preparedStatement.bind(keysToBind: _*))(session)
        val statement = preparedStatement.bind(keysToBind: _*).setFetchSize(1024)
        new ModifiedCassandraSourceStage(keysToBind.mkString(","), statement, session)
      })
  }
}
