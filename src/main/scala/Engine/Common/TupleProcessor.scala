package Engine.Common

import java.util

import Engine.Common.AmberTag.LayerTag
import Engine.Common.AmberTuple.Tuple

import scala.collection.immutable.HashMap

trait TupleProcessor extends TupleProducer {
  @throws(classOf[Exception])
  def accept(tuple:Tuple): Unit

  def onUpstreamChanged(from:LayerTag):Unit

  def onUpstreamExhausted(from:LayerTag):Unit

  def noMore():Unit

  def getBuildHashTable(): util.ArrayList[Any]

  def renewHashTable(hashTable: Any):Unit
}






