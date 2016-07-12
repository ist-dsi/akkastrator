package pt.tecnico.dsi.akkastrator

import scala.runtime.OrderedProxy

sealed trait Id extends Any {
  def self: Long
}
final class DeliveryId(val self: Long) extends AnyVal with OrderedProxy[Long] with Id {
  protected def ord = scala.math.Ordering.Long
}
final class CorrelationId(val self: Long) extends AnyVal with OrderedProxy[Long] with Id {
  protected def ord = scala.math.Ordering.Long
}

trait IdImplicits {
  implicit def longToDeliveryId(l: Long): DeliveryId = new DeliveryId(l)
  implicit def longToCorrelationId(l: Long): CorrelationId = new CorrelationId(l)
}
object IdImplicits extends IdImplicits