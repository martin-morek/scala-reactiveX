package protocols

import akka.actor.typed._
import akka.actor.typed.scaladsl._

object SelectiveReceive {

  /**
    * @return A behavior that stashes incoming messages unless they are handled
    *         by the underlying `initialBehavior`
    * @param bufferSize Maximum number of messages to stash before throwing a `StashOverflowException`
    *                   Note that 0 is a valid size and means no buffering at all (ie all messages should
    *                   always be handled by the underlying behavior)
    * @param initialBehavior Behavior to decorate
    * @tparam T Type of messages
    *
    * Hint: Implement an [[ExtensibleBehavior]], use a [[StashBuffer]] and [[Behavior]] helpers such as `start`,
    * `validateAsInitial`, `interpretMessage`,`canonicalize` and `isUnhandled`.
    */
  def apply[T](bufferSize: Int, initialBehavior: Behavior[T]): Behavior[T] = {
    import akka.actor.typed.ActorContext

    new ExtensibleBehavior[T]() {
      val buffer = StashBuffer[T](bufferSize)

      override def receive(ctx: ActorContext[T], msg: T): Behavior[T] = {
        import akka.actor.typed.Behavior.{canonicalize, interpretMessage, isUnhandled, start, validateAsInitial}

        val started = validateAsInitial(start(initialBehavior, ctx))
        val next = interpretMessage(started, ctx, msg)

        if (isUnhandled(next)) {

          buffer.stash(msg)
          Behaviors.same
        } else {
          buffer.unstashAll(
            ctx.asScala,
            SelectiveReceive(bufferSize, canonicalize(next, started, ctx)))
        }

      }

      override def receiveSignal(ctx: ActorContext[T],
                                 msg: Signal): Behavior[T] =
        Behavior.interpretSignal(Behaviors.same, ctx, msg)
    }
  }

}
