package pt.tecnico.dsi.akkastrator

sealed trait Event

case class StartOrchestrator(id: Long) extends Event

// We do not persist the request because:
//  1. The Task.createMessage is a function, and serializing functions is troublesome to say the least.
//  2. We do not need to. When an orchestrator is recreated it also recreates its Tasks which means the
//     request is available by invoking createMessage again.
//  3. We are not adding to the event journal possibly sensitive information like hashed passwords.
// This choice of implementation negates many of the benefits of EventSourcing such as inspecting previous requests.
// However the job of akkastrator is to restore the orchestrators whenever they crash and not provide EventSourcing capabilities. 
case class MessageSent(taskIndex: Int) extends Event
// Unfortunately we need to persist the response otherwise it would be impossible to compute whether to finish or abort.
case class MessageReceived(taskIndex: Int, response: Serializable) extends Event