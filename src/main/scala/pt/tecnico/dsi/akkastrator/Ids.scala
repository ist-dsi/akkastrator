package pt.tecnico.dsi.akkastrator

import scala.Proxy.Typed

sealed trait Id extends Any with Typed[Long]
final class DeliveryId(val self: Long) extends AnyVal with Ordered[DeliveryId] with Id {
  def compare(that: DeliveryId): Int = self.compare(that.self)
  
  override def toString = s"DeliveryId($self)"
}
final class CorrelationId(val self: Long) extends AnyVal with Ordered[CorrelationId] with Id {
  def compare(that: CorrelationId): Int = self.compare(that.self)
  
  override def toString = s"CorrelationId($self)"
}

trait IdImplicits {
  implicit def longToDeliveryId(l: Long): DeliveryId = new DeliveryId(l)
  implicit def longToCorrelationId(l: Long): CorrelationId = new CorrelationId(l)
}
object IdImplicits extends IdImplicits