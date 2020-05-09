package Engine.Common

import scala.concurrent.duration._

object Constants {
  val defaultBatchSize = 400
  var defaultNumWorkers = 0
  var masterNodeAddr:String = null

  var defaultTau: FiniteDuration = 10.milliseconds
}
