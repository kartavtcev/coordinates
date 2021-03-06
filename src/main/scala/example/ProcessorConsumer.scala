package example

import monix.eval.{Callback, Task}
import monix.execution.Ack.Continue
import monix.execution.Scheduler
import monix.execution.cancelables.AssignableCancelable
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber

import scala.concurrent.duration.Duration

object ProcessorConsumer {
  val value =
    new Consumer[Record, Either[String, List[Meet]]] {

      def createSubscriber(cb: Callback[Either[String, List[Meet]]], s: Scheduler) = {
        val out = new Subscriber.Sync[Record] {
          implicit val scheduler = s
          val processor = Processor.create.runSyncUnsafe(Duration.Inf)

          def onNext(elem: Record) = {
            processor.onNext(elem)
            Continue
          }

          def onComplete(): Unit = { // callback . onSuccess could only be called once. This is the only blocker for real time streaming.

            Task.sequence(Seq(processor.findMeetupsAsync, processor.meetups.read))
              .runAsync
              .foreach { case List((), m) =>
                val meetups = m.asInstanceOf[List[Meet]]
                if (!meetups.isEmpty) {
                  cb.onSuccess(Right(meetups))
                } else {
                  cb.onSuccess(Left("No meetups found."))
                }
              }
          }

          def onError(ex: Throwable): Unit = {
            cb.onError(ex)
          }
        }

        (out, AssignableCancelable.dummy)
      }
    }
}
