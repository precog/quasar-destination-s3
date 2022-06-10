/*
 * Copyright 2020 Precog Data
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.destination.s3

import slamdata.Predef._

import quasar.api.push.OffsetKey
import quasar.connector.DataEvent

import cats.effect.Concurrent
import cats.syntax.all._

import fs2.{Pipe, Pull, Stream}
import fs2.concurrent.Queue

class DataEventConsumer[F[_], A, B](queue: Queue[F, Option[OffsetKey.Actual[A]]], write: Pipe[F, B, Unit]) {

  private def go(inp: Stream[F, DataEvent[B, OffsetKey.Actual[A]]]): Pull[F, B, Unit] =
    inp.pull.uncons1 flatMap {
      case Some((ev, tail)) =>
        val act =
          ev match {
            case DataEvent.Create(chunk) => Pull.output(chunk)
            case DataEvent.Delete(ids) => Pull.pure(())
            case DataEvent.Commit(offset) => Pull.eval(queue.enqueue1(offset.some))
          }
        act >> go(tail)
      case None => Pull.done
    }

  def consume: Pipe[F, DataEvent[B, OffsetKey.Actual[A]], Unit] =
    go(_).stream.through(write)
}

object DataEventConsumer {

  def apply[F[_]: Concurrent, A, B](write: Pipe[F, B, Unit])
      : Pipe[F, DataEvent[B, OffsetKey.Actual[A]], OffsetKey.Actual[A]] =
    evs =>
      for {
        q <- Stream.eval(Queue.unbounded[F, Option[OffsetKey.Actual[A]]])

        consumer = new DataEventConsumer[F, A, B](q, write)

        offset <- q.dequeue.unNoneTerminate.concurrently {
          consumer.consume(evs).onFinalize(q.enqueue1(None))
        }
      } yield offset

}
