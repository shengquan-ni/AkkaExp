package Engine.Common

object Constants {
  val defaultBatchSize = 400
  val remoteHDFSPath = "hdfs://10.138.0.2:8020"
  @volatile var defaultNumWorkers = 0
  @volatile var dataset = 0
}
