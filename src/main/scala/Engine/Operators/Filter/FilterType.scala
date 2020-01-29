package Engine.Operators.Filter

import Engine.Common.AmberException.AmberException

import scala.language.postfixOps
import Ordering.Implicits._

object FilterType extends Enumeration with Serializable {

  case class Val[T:Ordering](validate: (T,T) => Boolean) extends super.Val with Serializable
  def Equal[T:Ordering]: Val[T] = Val[T]((x, y) => x==y)
  def Greater[T:Ordering]: Val[T] = Val[T]((x, y) => x>y)
  def GreaterOrEqual[T:Ordering]: Val[T] = Val[T]((x, y) => x>=y)
  def Less[T:Ordering]: Val[T] = Val[T]((x, y) => x<y)
  def LessOrEqual[T:Ordering]: Val[T] = Val[T]((x, y) => x<=y)
  def NotEqual[T:Ordering]: Val[T] = Val[T]((x, y) => x!=y)

  def getType[T:Ordering](string: String):Val[T] = {
    string match{
      case "Equal" => Equal
      case "Greater" => Greater
      case "GreaterOrEqual" => GreaterOrEqual
      case "Less" => Less
      case "LessOrEqual" => LessOrEqual
      case "NotEqual" => NotEqual
      case _ => throw new AmberException("filter type doesn't match with any exist type")
    }
  }
}
