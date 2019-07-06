/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
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

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case o: Operation => root ! o
    case GC => {
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))
    }
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case o: Operation => pendingQueue = pendingQueue.enqueue(o)
    case CopyFinished => {
      root = newRoot
      dequeue()
    }
  }

  def dequeue(): Unit = {
    pendingQueue.dequeueOption match {
      case None => context.become(normal)
      case Some((operation, queue)) => {
        pendingQueue = queue
        proxy(operation)
        context.become(processPending(operation))
      }
    }
  }

  def proxy(operation: Operation): Unit = {
    operation match {
      case Insert(_, id, elem) => root ! Insert(self, id, elem)
      case Contains(_, id, elem) => root ! Contains(self, id, elem)
      case Remove(_, id, elem) => root ! Remove(self, id, elem)
    }
  }

  def processPending(operation: Operation): Receive = {
    case o: Operation => pendingQueue = pendingQueue.enqueue(o)
    case or: OperationReply => {
      operation.requester ! or
      dequeue()
    }
  }
}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(requester, id, newElem) => onInsert(requester, id, newElem)
    case Contains(requester, id, newElem) => onContains(requester, id, newElem)
    case Remove(requester, id, newElem) => onRemove(requester, id, newElem)
    case CopyTo(newRoot) => onCopy(newRoot)
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(_) => {
      if (expected.isEmpty) {
        context.parent ! CopyFinished
        context.stop(self)
      } else {
        context.become(copying(expected, true))
      }
    }
    case CopyFinished => {
      val remaining = expected - sender()
      if (remaining.isEmpty && insertConfirmed) {
        context.parent ! CopyFinished
        context.stop(self)
      } else {
        context.become(copying(remaining, insertConfirmed))
      }
    }
  }

  def onInsert(requester: ActorRef, id: Int, newElem: Int): Unit = {
    if (newElem == elem) {
      if (removed) {
        removed = false
      }
      requester ! OperationFinished(id)
    } else if (newElem < elem) {
      // left
      subtrees.get(Left) match {
        case Some(child) => child ! Insert(requester, id, newElem)
        case None => {
          val child = context.actorOf(props(newElem, false))
          subtrees += (Left -> child)
          requester ! OperationFinished(id)
        }
      }
    } else {
      subtrees.get(Right) match {
        case Some(child) => child ! Insert(requester, id, newElem)
        case None => {
          val child = context.actorOf(props(newElem, false))
          subtrees += (Right -> child)
          requester ! OperationFinished(id)
        }
      }
    }
  }

  def onContains(requester: ActorRef, id: Int, newElem: Int): Unit = {
    if (newElem == elem) {
      requester ! ContainsResult(id, !removed)
    } else if (newElem < elem) {
      subtrees.get(Left) match {
        case Some(child) => child ! Contains(requester, id, newElem)
        case None => requester ! ContainsResult(id, false)
      }
    } else {
      subtrees.get(Right) match {
        case Some(child) => child ! Contains(requester, id, newElem)
        case None => requester ! ContainsResult(id, false)
      }
    }
  }

  def onRemove(requester: ActorRef, id: Int, newElem: Int): Unit = {
    if (newElem == elem) {
      removed = true
      requester ! OperationFinished(id)
    } else if (newElem < elem) {
      subtrees.get(Left) match {
        case Some(child) => child ! Remove(requester, id, newElem)
        case None => requester ! OperationFinished(id)
      }
    } else {
      subtrees.get(Right) match {
        case Some(child) => child ! Remove(requester, id, newElem)
        case None => requester ! OperationFinished(id)
      }
    }
  }

  def onCopy(node: ActorRef): Unit = {
    if (removed && subtrees.isEmpty) {
      // nothing to do here
      context.parent ! CopyFinished
      context.stop(self)
      return
    }

    if (!removed) {
      node ! Insert(self, elem, elem)
    }

    subtrees.foreach(e => e._2 ! CopyTo(node))

    // if it is removed, we don't need to confirm the insert
    context.become(copying(subtrees.values.toSet, removed))
  }
}
