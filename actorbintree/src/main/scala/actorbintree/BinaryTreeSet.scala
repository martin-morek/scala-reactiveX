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

  /** Request to perform garbage collection */
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
    //handle operations
    case Contains(requester, id, elem) => root ! Contains(requester, id, elem)
    case Insert(requester, id, elem) => root ! Insert(requester, id, elem)
    case Remove(requester, id, elem) => root ! Remove(requester, id, elem)

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
    //store operation for latter
    case Contains(requester, id, elem) => pendingQueue = pendingQueue.enqueue(Contains(requester, id, elem))
    case Insert(requester, id, elem) => pendingQueue = pendingQueue.enqueue(Insert(requester, id, elem))
    case Remove(requester, id, elem) => pendingQueue = pendingQueue.enqueue(Remove(requester, id, elem))

    case CopyFinished => {
      context.become(normal) //cleaning is done, continue with normal behavior
      root = newRoot //update root to new one
      pendingQueue.map(root ! _) // send pending operations to new root to process them
      pendingQueue = Queue.empty[Operation] // empty queue
    }
  }

}

object BinaryTreeNode {

  trait Position

  case object Left extends Position

  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)

  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
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
    case Contains(requester, id, e) => {
      if (!removed && e == elem) requester ! ContainsResult(id, true)
      else searchInSubTrees(requester, id, e)
    }
    case Insert(requester, id, e) => {
      if (e == elem) {
        requester ! OperationFinished(id)
        removed = false
      } else insertInToSubTree(requester, id, e)
    }
    case Remove(requester, id, e) => {
      if (e == elem) {
        requester ! OperationFinished(id)
        removed = true
      } else removeFromSubTree(requester, id, e)
    }

    case CopyTo(newRoot: ActorRef) => {
      val children: Set[ActorRef] = subtrees.values.toSet

      // actor doesn't have children and should be removed, remove now
      if (children.size == 0 && removed) {
        context.parent ! CopyFinished
        self ! PoisonPill
      }
      // actor has children and should be removed, send message to children and kill him when children are done
      if (children.size > 0 && removed) {
        children.map(_ ! CopyTo(newRoot)) //send message to copy to children
        context.become(copying(children, true)) //switch behavior
      }
      // actor should be copied, copy him and send message to children, kill him when children are done and coping is confirmed
      if (!removed) {
        children.map(_ ! CopyTo(newRoot)) //send message to copy to children
        newRoot ! Insert(self, elem, elem)
        context.become(copying(children, false)) //switch behavior
      }
    }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(_) => {
      if (expected.size == 0) {
        context.parent ! CopyFinished
        self ! PoisonPill
      } else context.become(copying(expected, true))
    }

    case CopyFinished => {
      val newExpected = expected - sender()

      if (newExpected.size == 0 && insertConfirmed) {
        context.parent ! CopyFinished
        self ! PoisonPill
      }
      context.become(copying(newExpected, insertConfirmed)) //switch behavior, remove from expected last replier
    }
  }

  private def selectSubtree(e: Int): (Option[ActorRef], Position) = {
    if (e < elem) // desired element is lower than actual, need to use left subtree
      (subtrees.get(Left), Left)
    else // desired element is bigger than actual, need to use right subtree
      (subtrees.get(Right), Right)
  }

  private def searchInSubTrees(requester: ActorRef, id: Int, e: Int) = {
    selectSubtree(e) match {
      case (Some(actor), _) => actor ! Contains(requester, id, e) //needs to go deeper
      case (None, _) => requester ! ContainsResult(id, false) //element not found, nowhere to go
    }
  }

  private def insertInToSubTree(requester: ActorRef, id: Int, e: Int) = {
    selectSubtree(e) match {
      case (Some(actor), _) => actor ! Insert(requester, id, e)
      case (None, position) => {
        subtrees = subtrees + (position -> context.actorOf(props(e, false))) // add new left actor to subtree map
        requester ! OperationFinished(id) // reply to requester
      }
    }
  }

  private def removeFromSubTree(requester: ActorRef, id: Int, e: Int) = {
    selectSubtree(e) match {
      case (Some(actor), _) => actor ! Remove(requester, id, e) //needs to go deeper
      case (None, _) => requester ! OperationFinished(id) //element not found, nowhere to go
    }
  }

}
