package Engine.Architecture.Worker

case class WorkerStatistics
(
workerState: WorkerState.Value,
outputRowCount: Int
)

