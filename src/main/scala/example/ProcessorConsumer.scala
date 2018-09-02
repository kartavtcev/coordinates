package example

import monix.eval.Callback
import monix.execution.Ack.Continue
import monix.execution.Scheduler
import monix.execution.cancelables.AssignableCancelable
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber

object ProcessorConsumer {
  val value =
    new Consumer[Record, Either[String, List[Meet]]] {

      def createSubscriber(cb: Callback[Either[String, List[Meet]]], s: Scheduler) = {
        val out = new Subscriber.Sync[Record] {
          implicit val scheduler = s
          val processor = new Processor

          def onNext(elem: Record) = {
            processor.onNext(elem)
            Continue
          }

          def onComplete(): Unit = {  // callback . onSuccess could only be called once. This is the only blocker for real time streaming.

            processor.findMeetupsAsync foreach { _ =>   // foreach runs Task async
              if (!processor.getMeetUps.isEmpty) {
                cb.onSuccess(Right(processor.getMeetUps))
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
