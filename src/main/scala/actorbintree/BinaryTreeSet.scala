/**
  * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
  */
package actorbintree

import akka.actor._
import akka.event.LoggingReceive

import scala.annotation.tailrec
import scala.collection.mutable.Queue

object BinaryTreeSet {

  sealed trait Operation {
    def requester: ActorRef

    def id: Int

    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor with akka.actor.ActorLogging {

  import BinaryTreeNode._
  import BinaryTreeSet._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional (used to stash incoming operations during garbage collection)
  var pendingQueue: Queue[Operation] = Queue.empty[Operation]

  // optional
  def receive = normal

  var isTreeEmpty = true

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = LoggingReceive {
    case GC =>
      val newRoot = createRoot
      context become garbageCollecting(newRoot)
      root ! CopyTo(newRoot)
    case op: Insert if isTreeEmpty =>
      insertRoot(op)
    case op: Operation =>
      root ! op
  }

  private def insertRoot(insert: Insert) = {
    context.stop(root)
    root = context.actorOf(BinaryTreeNode.props(insert.elem, initiallyRemoved = false))
    isTreeEmpty = false
    insert.requester ! OperationFinished(insert.id)

  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = LoggingReceive {
    case x: Operation => pendingQueue += x
    case CopyFinished =>
      root = newRoot
      processPendingQueue()
      context.become(normal)
  }

  @tailrec
  private def processPendingQueue(): Unit = {
    val op = pendingQueue.dequeue()
    root ! op
    if (pendingQueue.nonEmpty) processPendingQueue()
  }

}

object BinaryTreeNode {

  trait Position

  case object Left extends Position

  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)

  /**
    * Acknowledges that a copy has been completed. This message should be sent
    * from a node to its parent, when this node and all its children nodes have
    * finished being copied.
    */
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(new BinaryTreeNode(elem, initiallyRemoved))
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor with akka.actor.ActorLogging {

  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case x: Insert => handleInsert(x)
    case x: Contains => handleContains(x)
    case x: Remove => handleRemove(x)
    case CopyTo(_) if removed && subtrees.isEmpty => endCopy()
    case CopyTo(treeNode) =>
      context.become(copying(subtrees.values.toSet, removed))
      if (!removed) treeNode ! Insert(self, elem, elem)
      subtrees.values.foreach(_ ! CopyTo(treeNode))
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(waitingActors: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case CopyFinished if (waitingActors - sender).isEmpty && insertConfirmed => endCopy()
    case CopyFinished => context.become(copying(waitingActors - sender, insertConfirmed))
    case OperationFinished(`elem`) if waitingActors.isEmpty => endCopy()
    case OperationFinished(`elem`) => context.become(copying(waitingActors, insertConfirmed = true))
  }

  private def endCopy(): Unit = {
    context.parent ! CopyFinished
    self ! PoisonPill
  }

  private def handleInsert(insert: Insert): Unit = insert.elem match {
    case `elem` if !removed => insert.requester ! OperationFinished(insert.id)
    case _ =>
      val positionToInsert = if (elem > insert.elem) Left else Right
      subtrees.get(positionToInsert) match {
        case Some(child) => child ! insert
        case None =>
          subtrees += (positionToInsert -> context.actorOf(props(insert.elem, initiallyRemoved = false)))
          insert.requester ! OperationFinished(insert.id)
      }
  }

  private def handleContains(contains: Contains): Unit = contains.elem match {
    case `elem` if !removed =>
      contains.requester ! ContainsResult(contains.id, result = true)
    case _ =>
      subtrees.get(if (elem > contains.elem) Left else Right) match {
        case Some(childToAsk) =>
          childToAsk ! contains
        case None =>
          contains.requester ! ContainsResult(contains.id, result = false)
      }
  }

  private def handleRemove(remove: Remove): Unit = remove.elem match {
    case `elem` if !removed =>
      removed = true
      remove.requester ! OperationFinished(remove.id)
    case _ =>
      subtrees.get(if (elem > remove.elem) Left else Right) match {
        case Some(childToAsk) => childToAsk ! remove
        case None => remove.requester ! OperationFinished(remove.id)
      }
  }
}
