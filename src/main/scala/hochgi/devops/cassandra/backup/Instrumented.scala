package hochgi.devops.cassandra.backup

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.jmx.JmxReporter
import nl.grons.metrics4.scala.{InstrumentedBuilder, Meter}

class Instrumented(override val metricRegistry: MetricRegistry) extends InstrumentedBuilder {

  val ingestedRowRate: Meter = metrics.meter("ingestedRowRate",null)
  val ingestFailureRate: Meter = metrics.meter("ingestFailureRate",null)
  val ingestTotalFailureRate: Meter = metrics.meter("ingestTotalFailureRate",null)

  val reporter:  JmxReporter  = JmxReporter.forRegistry(metricRegistry).build()

  reporter.start()
}
