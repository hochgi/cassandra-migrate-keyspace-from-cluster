package hochgi.devops.cassandra.backup

import akka.NotUsed
import akka.stream.alpakka.cassandra.scaladsl.CassandraSource
import akka.stream.scaladsl.Source
import com.datastax.driver.core.{Row, Session}
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.collection.{breakOut, mutable}

object RemoteCassandraSource {

  def cassandraSource(conf: Config, session: Session): Source[Row,NotUsed] = {

    val myCodecRegistry = session.getCluster.getConfiguration.getCodecRegistry
    myCodecRegistry.register(new StringToDateCodec, new IntToBigintCodec)

    val stmt = conf.getString("hochgi.devops.cassandra.remote.select-statement")
    val preparedStatement = session.prepare(stmt)
    val numberOfKeysToBind = conf.getInt("hochgi.devops.cassandra.remote.key-arrity")
    require(numberOfKeysToBind > 0, s"No keys to bind select message can be used since defined key-arrity is [${numberOfKeysToBind}]")

    val listOfKeysToBind: List[List[AnyRef]] = {
      if (numberOfKeysToBind == 1) conf.getStringList(s"hochgi.devops.cassandra.remote.key-1").asScala.map(List.apply(_))(breakOut[mutable.Buffer[String], List[AnyRef], List[List[AnyRef]]])
      else (for (i <- 1 to numberOfKeysToBind) yield conf.getAnyRefList(s"hochgi.devops.cassandra.remote.key-$i").asScala.view.map {
          case r: AnyRef => r
          case d: Double => Double.box(d)
          case f: Float => Float.box(f)
          case l: Long => Long.box(l)
          case i: Int => Int.box(i)
          case c: Char => Char.box(c)
          case s: Short => Short.box(s)
          case b: Byte => Byte.box(b)
          case b: Boolean => Boolean.box(b)
        }.toList).toList
    }

    println(listOfKeysToBind.map(_.mkString("[", ",", "]")).mkString("[\n\t", ",\n\t", "]"))

    Source
      .fromIterator(() => Util.cartesianProduct(listOfKeysToBind))
      .grouped(conf.getInt("hochgi.devops.cassandra.remote.parallelism"))
      .flatMapConcat { keysToBindSeq =>
        val sources = keysToBindSeq.map { keysToBind =>
          println(s"Starting Cassandra source for [$stmt] with bindings ${keysToBind.mkString("[", ",", "]")}")
          CassandraSource(preparedStatement.bind(keysToBind: _*))(session)
        }
        interleaveSourcesRec(sources)
      }
  }

  def interleaveSourcesRec(sources: Seq[Source[Row,NotUsed]]): Source[Row,NotUsed] = {
    if(sources.isEmpty) Source.empty
    else if(sources.length == 1) sources.head
    else {
      val reduced = sources.sliding(2, 2).map { oneOrTwoSources =>
        if (oneOrTwoSources.length == 1) oneOrTwoSources.head
        else oneOrTwoSources.head.merge(oneOrTwoSources.last)
      }.toSeq
      interleaveSourcesRec(reduced)
    }
  }
}
