package pt.tecnico.dsi.akkastrator

sealed trait Id extends Any {
  def self: Long
}
final class DeliveryId(val self: Long) extends AnyVal with Id with Ordered[DeliveryId] {
  def compare(that: DeliveryId): Int = self.compare(that.self)
  override def toString = self.toString
}
final class CorrelationId(val self: Long) extends AnyVal with Id with Ordered[CorrelationId] {
  def compare(that: CorrelationId): Int = self.compare(that.self)
  override def toString = self.toString
}

trait IdImplicits {
  implicit def longToDeliveryId(l: Long): DeliveryId = new DeliveryId(l)
  implicit def longToCorrelationId(l: Long): CorrelationId = new CorrelationId(l)
}
object IdImplicits extends IdImplicits