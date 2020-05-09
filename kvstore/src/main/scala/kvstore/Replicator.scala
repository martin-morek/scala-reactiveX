package kvstore

import akka.actor.{Actor, ActorRef, Props, ReceiveTimeout}

import scala.concurrent.duration._

object Replicator {

  case class Replicate(key: String, valueOption: Option[String], id: Long)

  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)

  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {

  import Replicator._

  context.setReceiveTimeout(100.millis)

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks: Map[Long, (ActorRef, Replicate)] = Map.empty
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var _seqCounter = 0L

  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  def receive: Receive = {
    case Replicate(key, valueOption, id) => {
      val seqN = nextSeq()
      acks = acks + (seqN -> ((sender, Replicate(key, valueOption, id))))
      replica ! Snapshot(key, valueOption, seqN)
    }

    case SnapshotAck(key, seq) => {
      acks.get(seq).foreach {
        case (actor, request) => {
          actor ! Replicated(key, request.id)
          acks = acks - seq
        }
      }
    }

    case ReceiveTimeout =>
      acks.foreach {
        case (seq, (_, cmd)) =>
          replica ! Snapshot(cmd.key, cmd.valueOption, seq)
      }
  }

}
