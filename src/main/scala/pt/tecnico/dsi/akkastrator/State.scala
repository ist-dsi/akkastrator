package pt.tecnico.dsi.akkastrator

import akka.actor.ActorPath

import scala.collection.immutable.SortedMap

trait State
case object EmptyState extends State

trait DistinctIds { self: State ⇒
  //By using a SortedMap as opposed to a Map we can also extract the latest correlationId per sender
  //This must be a val to ensure the returned value is always the same.
  val idsPerDestination: Map[ActorPath, SortedMap[CorrelationId, DeliveryId]]

  /**
    * @return a new copy of State with the new IdsPerDestination.
    */
  def withIdsPerDestination(newIdsPerDestination: Map[ActorPath, SortedMap[CorrelationId, DeliveryId]]): State with DistinctIds

  /** @return the relations between CorrelationId and DeliveryId for the given `destination`. */
  final def getIdsFor(destination: ActorPath): SortedMap[CorrelationId, DeliveryId] = {
    idsPerDestination.getOrElse(destination, SortedMap.empty[CorrelationId, DeliveryId])
  }
  
  /**
    * Compute the next correlationId for the given `destination`.
    * The computation is just the biggest correlationId + 1 or 0 if no correlationId exists.
    */
  final def getNextCorrelationIdFor(destination: ActorPath): CorrelationId = {
    import pt.tecnico.dsi.akkastrator.IdImplicits._
    getIdsFor(destination)
      .keySet.lastOption
      .map[CorrelationId](_.self + 1L)
      .getOrElse(0L)
  }
  
  /**
    * Translates the `correlationId` for a given `destination` back to a DeliveryId.
    * If the translation is unsuccessful an exception will be thrown in order to crash the orchestrator
    * since this will be a fatal error.
    */
  private[akkastrator] def getDeliveryIdFor(destination: ActorPath, correlationId: CorrelationId): DeliveryId = {
    // You cannot see a Option.get here. You cannot see a Option.get here. You cannot see a Option.get here.
    idsPerDestination.get(destination).flatMap(_.get(correlationId)) match {
      case Some(deliveryId) ⇒ deliveryId
      case None ⇒ throw new IllegalArgumentException(
        s"""Could not obtain the delivery id for:
            |\tDestination: $destination
            |\tCorrelationId: $correlationId""".stripMargin)
    }
  }
  
  /**
    * @return a new copy of State with the IdsPerDestination updated for `destination` using the `newIdRelation`.
    */
  final def updatedIdsPerDestination(destination: ActorPath, newIdRelation: (CorrelationId, DeliveryId)): State with DistinctIds = {
    withIdsPerDestination(idsPerDestination.updated(destination, getIdsFor(destination) + newIdRelation))
  }
}

/**
  * The simplest implementation of a State with DistinctIds. The is the default state used by a DistinctIdsOrchestrator.
  */
case class MinimalState(idsPerDestination: Map[ActorPath, SortedMap[CorrelationId, DeliveryId]] = Map.empty) extends State with DistinctIds {
  def withIdsPerDestination(newIdsPerDestination: Map[ActorPath, SortedMap[CorrelationId, DeliveryId]]): MinimalState = {
    MinimalState(newIdsPerDestination)
  }
}