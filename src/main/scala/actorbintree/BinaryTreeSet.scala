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
    case Contains(actor, id, elem) => root ! Contains(actor, id, elem)
    case OperationFinished(id) => sender ! OperationFinished(id)
    case GC => ???
  }
  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  // optional

  // optional
  def receive = normal

  // optional

  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = ???

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
      println("***************this" + this.elem + "****")
      println("***************elem" + elem + "n****")

      elem match {
        case element if element == this.elem => actor ! OperationFinished(id)
        case element if element < this.elem => {
          if (subtrees.contains(Left))
            subtrees.get(Left).get ! Insert(actor, id, elem)
          else {
            val left = context.actorOf(BinaryTreeNode.props(elem, false))
            println("\n**************Inserting************LEFT " + left.path + "\n")
            subtrees = subtrees + (Left -> left)
            actor ! OperationFinished(id)
          }
        }
        case _ => {
          if (subtrees.contains(Right)) {
            subtrees.get(Right).get ! Insert(actor, id, elem)
          }
          else {
            val right = context.actorOf(BinaryTreeNode.props(elem, false))
            println("\n**************Inserting************" + right.path + "\n")
            subtrees = subtrees + (Right -> right)
            actor ! OperationFinished(id)
          }
        }
      }
    }
    case Contains(actor, id, elem) => {
      println("***************" + this.elem + "****")
      println("***************" + elem + "****")
      println("***************" + this.self + "****")
      println("***************" + subtrees.toString + "****")
        elem match {
          case element if element == this.elem => {
            if(!removed)
              actor ! ContainsResult(id, true)
            else
              actor ! ContainsResult(id, false)
          }
          case element if element < this.elem => {
            println("Going left")
            if (subtrees.contains(Left))
              subtrees.get(Left).get ! Contains(actor, id, elem)
            else
              actor ! ContainsResult(id, false)
          }
          case element if element > this.elem => {
            println("Going right")
            if (subtrees.contains(Right))
              subtrees.get(Right).get ! Contains(actor, id, elem)
            else
              actor ! ContainsResult(id, false)
          }
        }
    }
    case Remove(actor, id, elem) => {
      println("***************" + this.elem + "****")
      println("***************" + elem + "****")
      println("***************" + this.self + "****")
      println("***************" + subtrees.toString + "****")
      elem match {
        case elem if elem == this.elem => {
          this.removed = true
          actor ! OperationFinished(id)
        }
        case elem if elem < this.elem => {
          println("Going left")
          if (subtrees.contains(Left))
            subtrees.get(Left).get ! Remove(actor, id, elem)
          else
            OperationFinished(id)
        }
        case _ => {
          println("Going right")
          if (subtrees.contains(Right))
            subtrees.get(Right).get ! Remove(actor, id, elem)
          else
            actor ! OperationFinished(id)
        }
      }
    }
  }


  // optional

  // optional
  def receive = normal

  // optional

  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = ???


}
