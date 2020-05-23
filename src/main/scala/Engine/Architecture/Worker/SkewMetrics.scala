package Engine.Architecture.Worker

case class SkewMetrics(unprocessedQueueLength: Int, totalPutInInternalQueue: Int, stashedBatches: Int)
