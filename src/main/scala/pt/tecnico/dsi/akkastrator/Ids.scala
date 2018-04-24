package pt.tecnico.dsi.akkastrator

import scala.Proxy.Typed

sealed trait Id extends Any with Typed[Long]
final class DeliveryId(val self: Long) extends AnyVal with Id {
  override def toString: String = s"DeliveryId($self)"
}
final class CorrelationId(val self: Long) extends AnyVal with Ordered[CorrelationId] with Id {
  def compare(that: CorrelationId): Int = self.compare(that.self)
  
  override def toString: String = s"CorrelationId($self)"
}

trait IdImplicits {
  implicit def toDeliveryId(l: Long): DeliveryId = new DeliveryId(l)
  implicit def toCorrelationId(l: Long): CorrelationId = new CorrelationId(l)
}
object IdImplicits extends IdImplicits