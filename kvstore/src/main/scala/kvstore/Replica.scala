package kvstore

import akka.actor.{Actor, ActorRef, Cancellable, PoisonPill, Props}
import kvstore.Arbiter._

import scala.concurrent.duration._

object Replica {

  sealed trait Operation {
    def key: String

    def id: Long
  }

  case class Insert(key: String, value: String, id: Long) extends Operation

  case class Remove(key: String, id: Long) extends Operation

  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply

  case class OperationAck(id: Long) extends OperationReply

  case class OperationFailed(id: Long) extends OperationReply

  case class GetResult(key: String, valueOption: Option[String], id: Long)
      extends OperationReply

  case object ResendPersist

  case class DeadReplica(replicator: ActorRef)

  def props(arbiter: ActorRef, persistenceProps: Props): Props =
    Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {

  import Persistence._
  import Replica._
  import Replicator._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var resendTimer = context.system.scheduler.schedule(Duration.Zero,
                                                      100.millis,
                                                      self,
                                                      ResendPersist)

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  // expected snapshot sequence number
  var expectedSeqNumber = 0L
  // persistence actor
  var localPersistence: ActorRef = context.actorOf(persistenceProps)
  // map from id to persist command
  var persistAcks = Map.empty[Long, (Persist, Cancellable)]
  // map from id to set of replicators
  var replicatorsAcks = Map.empty[(String, Long), (Set[ActorRef], Cancellable)]
  // map from request id to client actor
  var taskToRequester = Map.empty[Long, ActorRef]
  //
  var replicatorRequests = Map.empty[ActorRef, Set[(Replicate, Cancellable)]]

  var replyTo = Map.empty[(String, Long), ActorRef]

  var replicatorsReplies =
    Map.empty[(String, Long), (Set[ActorRef], Cancellable)]

  var persistenceReply = Map.empty[(String, Long), Cancellable]

  override def preStart(): Unit = {
    arbiter ! Join
  }

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  // ###### ###### Helper methods ###### ###### //

  def setRequestToSender(key: String, id: Long, sender: ActorRef): Unit = {
    replyTo = replyTo + ((key, id) -> sender)
  }

  def getSenderForRequest(key: String, id: Long): Option[ActorRef] = {
    replyTo.get((key, id))
  }

  def setAwaitedReplicators(key: String, id: Long) = {
    replicatorsReplies = replicatorsReplies + ((key, id) -> ((replicators,
                                                              operationFailedTimer(
                                                                id))))
  }

  def removeReplicatorFromAwaited(key: String,
                                  id: Long,
                                  replicator: ActorRef) = {
    replicatorsReplies.get((key, id)) match {
      case Some((awaitedReplicators, timer)) => {
        val updatedReplicatorsSet = awaitedReplicators.intersect(replicators) - replicator

        if (updatedReplicatorsSet.isEmpty) {
          timer.cancel
          replicatorsReplies = replicatorsReplies - ((key, id))
        } else {
          replicatorsReplies = replicatorsReplies + ((key, id) -> ((updatedReplicatorsSet,
                                                                    timer)))
        }
        updatedReplicatorsSet
      }
    }
  }

  def getAwaitedReplicators(key: String, id: Long) = {
    replicatorsReplies.get((key, id)) match {
      case Some((awaitedReplicators, _)) => {
        awaitedReplicators
      }
      case _ => Set.empty[ActorRef]
    }
  }

  def addAwaitedPersistence(key: String, id: Long) = {
    persistenceReply = persistenceReply + ((key, id) -> operationFailedTimer(
      id))
  }

  def removeAwaitedPersistence(key: String, id: Long) = {
    persistenceReply.get((key, id)) foreach { timer =>
      timer.cancel
      persistenceReply = persistenceReply - ((key, id))
    }
  }

  def isPersist(key: String, id: Long): Boolean = {
    persistenceReply.get((key, id)) match {
      case Some(timer) if !timer.isCancelled => false
      case _                                 => true
    }
  }

  def operationFailedTimer(id: Long) =
    context.system.scheduler
      .scheduleOnce(1.second, sender, OperationFailed(id))

  // ###### ###### Behavior for primary Replica ###### ###### //

  val leader: Receive = {
    case Insert(key, value, id) => {
      setRequestToSender(key, id, sender)

      kv = kv + (key -> value)

      if (replicators.nonEmpty) setAwaitedReplicators(key, id)
      replicators.foreach(_ ! Replicate(key, Some(value), id))

      addAwaitedPersistence(key, id)
      localPersistence ! Persist(key, Some(value), id)
    }

    case Remove(key, id) => {
      setRequestToSender(key, id, sender)
      kv = kv - key

      setAwaitedReplicators(key, id)
      replicators.foreach(_ ! Replicate(key, None, id))

      addAwaitedPersistence(key, id)
      localPersistence ! Persist(key, None, id)
    }

    case Replicated(key, id) => {
      if (getAwaitedReplicators(key, id).nonEmpty) {
        val awaitedReplicators = removeReplicatorFromAwaited(key, id, sender)

        if (awaitedReplicators.isEmpty && isPersist(key, id)) {
          getSenderForRequest(key, id) foreach (_ ! OperationAck(id))
        }
      }
    }

    case DeadReplica(replicator: ActorRef) => {
      replicatorsReplies foreach {
        case ((key, id), (awaitedReplicators, timer)) => {
          val updatedReplicatorsSet = awaitedReplicators.intersect(replicators) - replicator

          if (updatedReplicatorsSet.isEmpty) {
            timer.cancel
            replicatorsReplies = replicatorsReplies - ((key, id))
            if (isPersist(key, id)) {
              getSenderForRequest(key, id) foreach (_ ! OperationAck(id))
            }
          } else {
            replicatorsReplies = replicatorsReplies + ((key, id) -> ((updatedReplicatorsSet,
                                                                      timer)))
          }
        }
      }
    }

    case Persisted(key, id) => {
      removeAwaitedPersistence(key, id)

      if (getAwaitedReplicators(key, id).isEmpty) {
        getSenderForRequest(key, id) foreach (_ ! OperationAck(id))
      }
    }

    case ResendPersist => {
      persistenceReply.keySet.foreach {
        case (key, id) => {
          localPersistence ! Persist(key, kv.get(key), id)
        }
      }
    }

    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    // handle secondary replicas changes
    case Replicas(replicas) => {
      val newSecReplicas = replicas - self
      val actualSecReplicas = secondaries.keySet

      val needToBeTerminated = actualSecReplicas.diff(newSecReplicas)
      val needToBCreated = newSecReplicas.diff(actualSecReplicas)

      for (replica <- needToBeTerminated) yield {
        secondaries.get(replica) match {
          case Some(replicator) => {
            self ! DeadReplica(replicator)
            replicator ! PoisonPill
            replicators = replicators - replicator
          }
        }
      }

      for (newReplica <- needToBCreated) yield {
        val newReplicator = context.actorOf(Replicator.props(newReplica))
        secondaries = secondaries + (newReplica -> newReplicator)
        replicators = replicators + newReplicator

        // forward all items in kv map to new replicator
        for (((key, value), index) <- kv.zipWithIndex)
          yield newReplicator ! Replicate(key, Some(value), index)
      }
    }
  }

  // ###### ###### Behavior for secondary Replica ###### ###### //

  val replica: Receive = {
    case Snapshot(key, _, seq) if seq < expectedSeqNumber => {
      sender ! SnapshotAck(key, seq)
    }

    case Snapshot(key, valueOption, seq) if seq == expectedSeqNumber => {
      setRequestToSender(key, seq, sender)
      expectedSeqNumber = expectedSeqNumber + 1

      valueOption match {
        case Some(v) => kv = kv + (key -> v)
        case None    => kv = kv - key
      }

      addAwaitedPersistence(key, seq)
      localPersistence ! Persist(key, valueOption, seq)
    }

    case Persisted(key, id) => {
      removeAwaitedPersistence(key, id)

      getSenderForRequest(key, id) foreach (_ ! SnapshotAck(key, id))
    }

    case ResendPersist => {
      persistenceReply.keySet.foreach {
        case (key, id) => {
          localPersistence ! Persist(key, kv.get(key), id)
        }
      }
    }

    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)
  }

}
