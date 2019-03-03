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

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

  /** Request to perform garbage collection */
  case object GC

}


class BinaryTreeSet extends Actor {

  import BinaryTreeSet._
  import BinaryTreeNode._


  /** Accepts `Operation` and `GC` messages. */
  //val normal: Receive = { case _ => ??? }
  val normal: Receive = {
    case Insert(ac, id, elem) => root ! Insert(ac, id, elem)
    case Remove(actor, id, elem) => root ! Remove(actor, id, elem)
    case Contains(actor, id, elem) => {
      println("Set Rceiving contains")
      root ! Contains(actor, id, elem)
    }
    case OperationFinished(id) => sender ! OperationFinished(id)
    case GC => {
      println("Garbage Collecting")
      val newroot = createRoot
      context.become(garbageCollecting(newroot))
      root ! CopyTo(newroot)
    }

  }

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  def createRoot: ActorRef = {

   val r = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))
    println("The root is " + r.hashCode())
    r
  }

  // optional

  // optional
  def receive = normal

  // optional

  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newroot: ActorRef): Receive = {
    case CopyFinished =>
      println("****************ROOT RECEIVING COPY FINISH")
      root = newroot
      context.become(receive)
      for (operation: Operation <- pendingQueue)
        root ! operation
    case operation: Operation =>{
      pendingQueue = pendingQueue.enqueue(operation)
      //log.info("{} receive Operation {} in GCollecting Mode. Queue Size:{}", self, operation.id, pendingQueue.size)
    }
  }

}

object BinaryTreeNode {
  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)

  trait Position

  case class CopyTo(treeNode: ActorRef)

  case object Left extends Position

  case object Right extends Position

  case object CopyFinished

}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {

  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees: Map[Position, ActorRef] = Map[Position, ActorRef]()
  var removed: Boolean = initiallyRemoved
  /** Handles `Operation` messages and `CopyTo` requests. */
  //  val normal: Receive = { case _ => ??? }



  val normal: Receive = {
    case Insert(actor, id, elem) => {
      //println("Inserting value: " + elem + " id: " + id)
      elem match {
        case element if element == this.elem =>
          this.removed = false
          actor ! OperationFinished(id)
        case element if element < this.elem => {
          if (subtrees.contains(Left))
            subtrees.get(Left).get ! Insert(actor, id, elem)
          else {
            val left = context.actorOf(BinaryTreeNode.props(elem, false))
            subtrees = subtrees + (Left -> left)
            println("Inserted id: " + id + " element:" + elem)
            actor ! OperationFinished(id)
          }
        }
        case _ => {
          if (subtrees.contains(Right)) {
            subtrees.get(Right).get ! Insert(actor, id, elem)
          }
          else {
            val right = context.actorOf(BinaryTreeNode.props(elem, false))
            subtrees = subtrees + (Right -> right)
            println("Inserted id: " + id + " element:" + elem)
            actor ! OperationFinished(id)
          }
        }
      }
    }
    case Contains(actor, id, elem) => {
        println("looking up result: " + elem + " id: " + id)
        elem match {
          case element if element == this.elem => {
              if(this.removed)
                actor ! ContainsResult(id, false)
              else
                actor ! ContainsResult(id, true)
          }
          case element if element < this.elem => {
            if (subtrees.contains(Left))
              subtrees.get(Left).get ! Contains(actor, id, elem)
            else {
              println("Not found id: " + id + " element: " + elem)
              actor ! ContainsResult(id, false)
            }
          }
          case element if element > this.elem => {
            if (subtrees.contains(Right))
              subtrees.get(Right).get ! Contains(actor, id, elem)
            else {
              println("Not found id: " + id + " element: " + elem)
              actor ! ContainsResult(id, false)
            }
          }
        }
    }
    case Remove(actor, id, elem) => {
//      println("Removing element " + elem + " My current element is " + this.elem + " and my remove value is currently " + this.removed +
//        " Contains left is " + subtrees.contains(Left) + " and Contains Right is " + subtrees.contains(Right) + "the id is " + id)
      elem match {
        case element if element == this.elem => {
          this.removed = true
          println("Removed element: " + elem + " id" + id)
          actor ! OperationFinished(id)
        }
        case element if element < this.elem => {
          if (subtrees.contains(Left))
            subtrees.get(Left).get ! Remove(actor, id, elem)
          else {
            println("Removed not found returning element: " + elem + " id" + id)
            actor ! OperationFinished(id)
          }
        }
        case _ => {
          if (subtrees.contains(Right))
            subtrees.get(Right).get ! Remove(actor, id, elem)
          else {
            println("Removed not found returning element: " + elem + " id" + id)
            actor ! OperationFinished(id)
          }
        }
      }
    }
    case CopyTo(treeNode) => {
      println("Copy to started")
      context.become(copying(sender, subtrees.values.toSet, removed))
      for(actor: ActorRef <- subtrees.values.toSet) {
        actor ! CopyTo(treeNode)
      }
      if(!removed)
        treeNode ! Insert(self, this.hashCode,this.elem)
      if(subtrees.isEmpty)
        sender ! CopyFinished
    }
}


  // optional

  // optional
  def receive = normal

  // optional

  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(sentby: ActorRef, expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(_) => {
      context.become(copying(sentby, expected, true))
      println("Set is" + expected + " insertConfirmed is: " + insertConfirmed)
        if(expected.isEmpty) {
          sentby ! CopyFinished
          context.stop(self)
      }
    }
    case CopyFinished => {
      println("Terminate received")
      val newSet = expected.filter((x) => x != sender)
      println("OLD SET IS " + expected + " New set is " + newSet)
      if(newSet.isEmpty && insertConfirmed){
        println("MY element is " + this.elem)
        println("Sending copy finished to " + sentby)
        sentby ! CopyFinished
        context.stop(self)
      }else {
        context.become(copying(sentby, newSet,insertConfirmed))
    }
    }
  }


}
