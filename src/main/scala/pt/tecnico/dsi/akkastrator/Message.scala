package pt.tecnico.dsi.akkastrator

object Message {
  type MessageId = Long

  /**
   * Base "type" for all messages (either Requests or Responses) exchanged
   * between the orchestrator's commands and the actors they talk to.
   *
   * We are using a structural type so that an Orchestrator maybe be used with any library actor.
   * The alternative would be to define Message as a trait. However this would limit to which actors the
   * orchestrator could talk to since those actors would have to exchange messages extending the Message trait.
   */
  type Message = { def id: MessageId }
}