package filodb.downsampler

import com.datastax.driver.core.Session
import com.typesafe.config.Config
import com.typesafe.scalalogging.{Logger, StrictLogging}
import monix.execution.Scheduler

import filodb.cassandra.FiloSessionProvider
import filodb.core.concurrentCache

object DownsamplerContext extends StrictLogging {
  lazy val dsLogger: Logger = logger
  lazy val sessionMap = concurrentCache[Config, Session](2)

  lazy val readSched = Scheduler.io("cass-read-sched")
  lazy val writeSched = Scheduler.io("cass-write-sched")

  def getOrCreateCassandraSession(config: Config): Session = {
    import filodb.core._
    sessionMap.getOrElseUpdate(config, { conf =>
      dsLogger.info(s"Creating new Cassandra session")
      FiloSessionProvider.openSession(conf)
    })
  }
}
