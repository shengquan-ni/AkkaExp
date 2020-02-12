package Engine.Common.AmberMessage

import Engine.Architecture.Breakpoint.GlobalBreakpoint.GlobalBreakpoint
import Engine.Architecture.DeploySemantics.Layer.ActorLayer
import Engine.Architecture.LinkSemantics.LinkStrategy
import Engine.Architecture.Principal.PrincipalState
import Engine.Common.AmberTag.{AmberTag, LayerTag, WorkerTag}
import Engine.Operators.OperatorMetadata

object PrincipalMessage{
  final case class AckedPrincipalInitialization(prev:Array[(OperatorMetadata,ActorLayer)])

  final case class GetInputLayer()

  final case class GetOutputLayer()

  final case class AppendLayer(linkStrategy: LinkStrategy)

  final case class PrependLayer(prev:Array[(OperatorMetadata,ActorLayer)], linkStrategy: LinkStrategy)

  final case class AssignBreakpoint(breakpoint:GlobalBreakpoint)

  final case class ReportState(principalState: PrincipalState.Value)

  final case class ReportPrincipalPartialCompleted(from:AmberTag,layer:LayerTag)


}

