package web.model.request

import scala.beans.BeanProperty

case class HelloWorldRequest(@BeanProperty message: String) extends TexeraWsRequest
