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

  /**
    * Get the SorteMap relation between CorrelationId and DeliveryId for the given `destination`.
    */
  def getIdsFor(destination: ActorPath): SortedMap[CorrelationId, DeliveryId] = {
    idsPerDestination.getOrElse(destination, SortedMap.empty[CorrelationId, DeliveryId])
  }

  /**
    * @return a new copy of State with the IdsPerDestination updated for `destination` using the `newIdRelation`.
    */
  def updatedIdsPerDestination(destination: ActorPath, newIdRelation: (CorrelationId, DeliveryId)): State with DistinctIds = {
    withIdsPerDestination(idsPerDestination.updated(destination, getIdsFor(destination) + newIdRelation))
  }
}

class MinimalState(val idsPerDestination: Map[ActorPath, SortedMap[CorrelationId, DeliveryId]] = Map.empty) extends State with DistinctIds {
  def withIdsPerDestination(newIdsPerDestination: Map[ActorPath, SortedMap[CorrelationId, DeliveryId]]): MinimalState = {
    new MinimalState(newIdsPerDestination)
  }
}