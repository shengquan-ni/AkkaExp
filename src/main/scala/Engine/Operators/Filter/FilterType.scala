package Engine.Operators.Filter

import scala.language.postfixOps
import Ordering.Implicits._

object FilterType extends Enumeration {

  case class Val[T:Ordering](validate: (T,T) => Boolean) extends super.Val
  def Equal[T:Ordering]: Val[T] = Val[T]((x, y) => x==y)
  def Greater[T:Ordering]: Val[T] = Val[T]((x, y) => x>y)
  def GreaterOrEqual[T:Ordering]: Val[T] = Val[T]((x, y) => x>=y)
  def Less[T:Ordering]: Val[T] = Val[T]((x, y) => x<y)
  def LessOrEqual[T:Ordering]: Val[T] = Val[T]((x, y) => x<=y)
  def NotEqual[T:Ordering]: Val[T] = Val[T]((x, y) => x!=y)
}
