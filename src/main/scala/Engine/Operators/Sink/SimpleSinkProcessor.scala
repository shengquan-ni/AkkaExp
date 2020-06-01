package Engine.Operators.Sink

import java.util

import Engine.Common.AmberTag.LayerTag
import Engine.Common.AmberTuple.Tuple
import Engine.Common.TupleProcessor

class SimpleSinkProcessor extends TupleProcessor{
  override def accept(tuple: Tuple): Unit = {
    println("Sink################: "+ tuple.toString)
  }

  override def noMore(): Unit = {

  }

  override def initialize(): Unit = {

  }

  override def hasNext: Boolean = false

  override def next(): Tuple = {
    throw new NotImplementedError()
  }

  override def dispose(): Unit = {

  }

  override def onUpstreamChanged(from: LayerTag): Unit = {

  }

  override def onUpstreamExhausted(from: LayerTag): Unit = {

  }

  override def getBuildHashTable: util.ArrayList[Any] = null

  override def renewHashTable(hashTable: Any): Unit = {
  }
}
