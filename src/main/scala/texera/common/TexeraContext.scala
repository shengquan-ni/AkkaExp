package texera.common

import java.lang.NumberFormatException

import javax.validation.Validator

import scala.collection.mutable

class TexeraContext {
  var workflowID: String = null
  @transient var validator: javax.validation.Validator = null
  var customFieldIndexMapping: Map[String, Integer] = _

  def fieldIndexMapping(field: String): Integer = {
    if (customFieldIndexMapping != null) {
      customFieldIndexMapping(field.toLowerCase.trim)
    } else {
      try {
        Integer.parseInt(field.trim())
      } catch {
        case n: NumberFormatException => null
        case e: Throwable => throw(e)
      }
    }
  }
}