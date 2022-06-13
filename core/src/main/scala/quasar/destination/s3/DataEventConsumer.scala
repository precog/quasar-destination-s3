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

import cats.effect.{Concurrent, Sync}
import cats.syntax.all._

import fs2.{Pipe, Stream}
import fs2.concurrent.Queue
import org.slf4s.Logger

class DataEventConsumer[F[_], A, B](
  logger: Logger,
  queue: Queue[F, Option[OffsetKey.Actual[A]]],
  write: Pipe[F, B, Unit])(
  implicit F: Sync[F]) {

  private def pipe: Pipe[F, DataEvent[B, OffsetKey.Actual[A]], B] =
    _.flatMap {
      case DataEvent.Create(chunk) => Stream.chunk(chunk)
      case DataEvent.Delete(ids) => Stream.eval(F.delay(logger.warn(s"Ignoring delete event with ids: ${ids.show}"))).drain
      case DataEvent.Commit(offset) => Stream.eval(queue.enqueue1(offset.some)).drain
    }

  def consume: Pipe[F, DataEvent[B, OffsetKey.Actual[A]], Unit] =
    pipe.andThen(write)
}

object DataEventConsumer {

  def apply[F[_]: Concurrent, A, B](logger: Logger, write: Pipe[F, B, Unit])
      : Pipe[F, DataEvent[B, OffsetKey.Actual[A]], OffsetKey.Actual[A]] =
    evs =>
      for {
        q <- Stream.eval(Queue.unbounded[F, Option[OffsetKey.Actual[A]]])

        consumer = new DataEventConsumer[F, A, B](logger, q, write)

        offset <- q.dequeue.unNoneTerminate.concurrently {
          consumer.consume(evs).onFinalize(q.enqueue1(None))
        }
      } yield offset

}
