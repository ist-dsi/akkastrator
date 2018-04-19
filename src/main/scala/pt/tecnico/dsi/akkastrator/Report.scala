package pt.tecnico.dsi.akkastrator

import scala.collection.immutable.Seq

import akka.actor.ActorPath

// TODO find a better name for this class
/**
  * An immutable representation (a report) of a Task in a given moment of time.
  *
  * @param index the task index this report pertains to.
  * @param description a text that describes the task in a human readable way. Or a message key to be used in internationalization.
  * @param dependencies the indexes of the tasks that must have finished in order for the task to be able to start.
  * @param state the current state of the task.
  * @param destination the destination of the task. If the task hasn't started this will be a None.
  * @param result the result of the task. If the task hasn't finished this will be a None.
  * @tparam R the type of the result.
  */
case class Report[R](index: Int, description: String, dependencies: Seq[Int], state: Task.State, destination: Option[ActorPath], result: Option[R])