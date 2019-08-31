/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("test")
    val client = system.actorOf(Props(classOf[BinaryTreeSet]))

    client.tell(Insert(client, id=100, elem=1), client)
    client.tell(Contains(client, id=50, elem=2), client)
    client.tell(Contains(client, id=60, elem=1), client)
    client.tell(Remove(client, id=70, elem=1), client)
    client.tell(Contains(client, id=80, elem=1), client)
    client.tell(Insert(client, id=90, elem=1), client)
    client.tell(Contains(client, id=110, elem=1), client)
    client.tell(Remove(client, id=120, elem=1), client)
    client.tell(Insert(client, id=130, elem=2), client)
    client.tell(Contains(client, id=140, elem=1), client)
    client.tell(Contains(client, id=150, elem=2), client)
  }

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
  def receive= normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case OperationFinished(id) => onOperationFinished(OperationFinished(id))
    case ContainsResult(id, res) => onOperationFinished(ContainsResult(id, res))
    case op: Operation => handleOperation(op)
  }

  private def handleOperation(op: Operation): Unit = {
    if(pendingQueue.isEmpty) {
      root.tell(op, op.requester)
    } else {
      pendingQueue = pendingQueue.enqueue(op)
    }
  }

  private def printReply: (OperationReply) => Unit = {
    case OperationFinished(id) => printf("Operation finished: { id: %s }\n", id)
    case ContainsResult(id, res) => printf("Contains result: { id: %s, response: %s }\n", id, res)
  }

  private def processNext: Unit = {
    if (!pendingQueue.isEmpty) {
      val (nextOp, newQueue) = pendingQueue.dequeue
      receive(nextOp)

      pendingQueue = newQueue
    }
  }

  private def onOperationFinished(op: OperationReply): Unit = {
    printReply(op)
    processNext
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = ???

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

  def handleInsert(op: Insert): Unit = {
    if (op.elem == elem) {
      if (removed) {
        removed = false
      }

      op.requester.tell(OperationFinished(op.id), self)
    } else if (op.elem > elem && subtrees.contains(Right)) {
      subtrees(Right).forward(op)
    } else if (op.elem < elem && subtrees.contains(Left)) {
      subtrees(Left).forward(op)
    } else {
      val newNode = context.actorOf(BinaryTreeNode.props(op.elem, initiallyRemoved = false))
      val position: Position = if (op.elem > elem) Right else Left

      subtrees += (position -> newNode)

      op.requester.tell(OperationFinished(op.id), self)
    }
  }

  def handleContains(op: Operation): Unit = {
    if (op.elem == elem) {
      op.requester.tell(ContainsResult(op.id, !removed), self)
    } else if (op.elem > elem && subtrees.contains(Right)) {
      subtrees(Right).forward(op)
    } else if (op.elem < elem && subtrees.contains(Left)) {
      subtrees(Left).forward(op)
    } else {
      op.requester.tell(ContainsResult(op.id, false), self)
    }
  }

  def handleRemove(op: Operation): Unit = {
    if (op.elem == elem && !removed) {
      removed = true
      op.requester.tell(OperationFinished(op.id), self)
    } else if (op.elem > elem && subtrees.contains(Right)) {
      subtrees(Right).forward(op)
    } else if (op.elem < elem && subtrees.contains(Left)) {
      subtrees(Left).forward(op)
    } else {
      op.requester.tell(OperationFinished(op.id), self)
    }
  }

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(requester, id, newElem) => handleInsert(Insert(requester, id, newElem))
    case Contains(requester, id, elemToFind) => handleContains(Contains(requester, id, elemToFind))
    case Remove(requester, id, elemToRemove) => handleRemove(Remove(requester, id, elemToRemove))
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = ???


}
