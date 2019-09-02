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

    client.tell(Insert(client, id=1, elem=1), client)
    client.tell(Contains(client, id=2, elem=2), client)
    client.tell(Contains(client, id=3, elem=1), client)
    client.tell(Remove(client, id=4, elem=1), client)
    client.tell(Contains(client, id=5, elem=1), client)
    client.tell(Insert(client, id=6, elem=1), client)
    client.tell(Contains(client, id=7, elem=1), client)
    client.tell(Remove(client, id=8, elem=1), client)
    client.tell(Insert(client, id=9, elem=2), client)
    client.tell(Contains(client, id=10, elem=1), client)
    client.tell(Contains(client, id=1, elem=2), client)
    client.tell(Remove(client, id=12, elem=1), client)

    client.tell(GC, client)

    client.tell(Insert(client, id=20, elem=1), client)
    client.tell(Contains(client, id=22, elem=1), client)
    client.tell(Contains(client, id=23, elem=2), client)
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
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case OperationFinished(id) => {
      printReply(OperationFinished(id))
      processNext
    }
    case ContainsResult(id, res) => {
      printReply(ContainsResult(id, res))
      processNext
    }
    case op: Operation => root.tell(op, op.requester)
    case GC => {
      val newRoot = createRoot
      root.tell(CopyTo(newRoot), self)
      context.become(garbageCollecting(newRoot))
    }
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case op: Operation => {
      printf("Operation received during GC: { id: %s, queueSize: ", op.id)
      pendingQueue = pendingQueue.enqueue(op)
      printf("%s }\n", pendingQueue.size)
    }
    case CopyFinished => {
      println("Manager copy finished")
      root = newRoot
      context.become(normal)
      processNext
    }
    case GC => printf("Already garbage collection")
    case OperationFinished(id) => printReply(OperationFinished(id))
    case ContainsResult(id, res) => printReply(ContainsResult(id, res))
  }

  private def printReply: (OperationReply) => Unit = {
    case OperationFinished(id) => printf("Operation finished: { id: %s }\n", id)
    case ContainsResult(id, res) => printf("Contains result: { id: %s, response: %s }\n", id, res)
  }

  private def processNext: Unit = {
    if (!pendingQueue.isEmpty) {
      val (nextOp, newQueue) = pendingQueue.dequeue
      pendingQueue = newQueue
      receive(nextOp)
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

  def receive = normal

  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(requester, id, newElem) => handleInsert(Insert(requester, id, newElem))
    case Contains(requester, id, elemToFind) => handleContains(Contains(requester, id, elemToFind))
    case Remove(requester, id, elemToRemove) => handleRemove(Remove(requester, id, elemToRemove))
    case CopyTo(treeNode) => {
      printf("Received copy to, { elem: %s, removed: %s }\n", elem, removed)

      if (context.children.isEmpty && this.removed) { // No work to do
        context.parent ! CopyFinished
      } else {
        // copy self
        if(!removed) {
          treeNode ! Insert(self, -1, elem)
        }

        // copy children
        for {
          child <- context.children
        } child ! CopyTo(treeNode)

        // await copies to finish
        context.become(copying(context.children.toSet, removed))
      }
    }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    // child completed
    case CopyFinished => {
      val newExpected = expected - context.sender
      // printf("Copy finished, { elem: %s, insertConfirmed: %s, childrenSize: %s }\n", elem, insertConfirmed, newExpected.size)
      // done copying
      if(insertConfirmed && newExpected.isEmpty) {
        context.parent ! CopyFinished
      } else {
        // waiting for parent or children
        context.become(copying(newExpected, insertConfirmed))
      }
    }
    // self completed
    case OperationFinished(_) => {
//      printf("Operation finished, { elem: %s, expectedSize: %s }\n", elem, expected.size)
      // no children to wait for
      if(expected.isEmpty) {
        context.parent ! CopyFinished
      } else {
        // current node copied but children to wait for
        context.become(copying(expected, true))
      }
    }
  }

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

      op.requester ! OperationFinished(op.id)
    }
  }

  def handleContains(op: Contains): Unit = {
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

  def handleRemove(op: Remove): Unit = {
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
}
