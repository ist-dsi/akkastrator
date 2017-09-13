package pt.tecnico.dsi.akkastrator

import akka.actor.ActorPath

import scala.collection.immutable.SortedMap

sealed trait State
case object EmptyState extends State

//By using a SortedMap as opposed to a Map we can also extract the latest correlationId per sender
final case class DistinctIdsState(idsPerDestination: Map[ActorPath, SortedMap[CorrelationId, DeliveryId]] = Map.empty) extends State {
  /** @return the relations between CorrelationId and DeliveryId for the given `destination`. */
  def idsOf(destination: ActorPath): SortedMap[CorrelationId, DeliveryId] = {
    idsPerDestination.getOrElse(destination, SortedMap.empty[CorrelationId, DeliveryId])
  }
  
  /**
    * Compute the next correlationId for the given `destination`.
    * The computation is just the biggest correlationId + 1 or 0 if no correlationId exists.
    */
  def nextCorrelationIdFor(destination: ActorPath): CorrelationId = {
    import IdImplicits._
    idsOf(destination)
      .keySet.lastOption
      .map[CorrelationId](_.self + 1L)
      .getOrElse(0L)
  }
  
  /**
    * Translates the `correlationId` for a given `destination` back to a DeliveryId.
    * If the translation is unsuccessful an exception will be thrown in order to crash the orchestrator
    * since this will be a fatal error.
    */
  private[akkastrator] def deliveryIdOf(destination: ActorPath, correlationId: CorrelationId): DeliveryId = {
    // While a Jedi waves his hand in the air: you cannot see a Option.get here.
    idsPerDestination.get(destination).flatMap(_.get(correlationId)) match {
      case Some(deliveryId) => deliveryId
      case None => throw new IllegalArgumentException(
        s"""Could not obtain the delivery id for:
            |\tDestination: $destination
            |\tCorrelationId: $correlationId""".stripMargin)
    }
  }
  
  /**
    * @return a new copy of State with the IdsPerDestination updated for `destination` using the `newIdRelation`.
    */
  def updatedIdsPerDestination(destination: ActorPath, newIdRelation: (CorrelationId, DeliveryId)): DistinctIdsState = {
    this.copy(idsPerDestination.updated(destination, idsOf(destination) + newIdRelation))
  }
}