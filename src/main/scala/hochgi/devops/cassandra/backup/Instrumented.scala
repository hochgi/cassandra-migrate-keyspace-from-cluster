package hochgi.devops.cassandra.backup

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.jmx.JmxReporter
import nl.grons.metrics4.scala.InstrumentedBuilder

class Instrumented(override val metricRegistry: MetricRegistry) extends InstrumentedBuilder {

  val ingestedRowRate = metrics.meter("ingestedRowRate",null)

  val reporter:  JmxReporter  = JmxReporter.forRegistry(metricRegistry).build()
  reporter.start()
}
