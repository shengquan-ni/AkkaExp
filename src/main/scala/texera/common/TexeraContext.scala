package texera.common

class TexeraContext {
  var workflowID: String = null
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