package Engine.Operators.Filter

import java.util

import Engine.Common.AmberTag.LayerTag
import Engine.Common.AmberTuple.Tuple
import Engine.Common.{TableMetadata, TupleProcessor}
import com.github.nscala_time.time.Imports._

class FilterSpecializedTupleProcessor(val targetField:Int, val filterType:Int, val threshold:DateTime) extends TupleProcessor {
  var _tuple:Tuple = _
  var nextFlag = false

  override def accept(tuple: Tuple): Unit = {
    val str = tuple.getString(targetField)
    if (str!=null && DateTime.parse(str) > threshold) {
      nextFlag = true
      _tuple = tuple
    }
  }

  override def noMore(): Unit = {

  }

  override def hasNext: Boolean = nextFlag

  override def next(): Tuple = {
    nextFlag = false
    _tuple
  }

  override def dispose(): Unit = {

  }

  override def initialize(): Unit = {

  }

  override def onUpstreamChanged(from: LayerTag): Unit = {

  }

  override def onUpstreamExhausted(from: LayerTag): Unit = {

  }

  override def getBuildHashTable: util.ArrayList[Any] = null

  override def renewHashTable(hashTable: Any): Unit = {
  }

}

