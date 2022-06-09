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

import quasar.EffectfulQSpec
import quasar.api.Column
import quasar.api.push.{OffsetKey, PushColumns}
import quasar.api.resource.{ResourceName, ResourcePath}
import quasar.blobstore.s3.{Bucket, ObjectKey}
import quasar.blobstore.paths.BlobPath
import quasar.connector.{AppendEvent, DataEvent, ResourceError}
import quasar.connector.destination.{ResultSink, WriteMode}
import quasar.contrib.scalaz.MonadError_

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global

import cats.data.NonEmptyList
import cats.effect.concurrent.Ref
import cats.effect.{IO, Timer}
import cats.implicits._
import fs2.{Chunk, Stream, text}
import org.slf4s.LoggerFactory
import skolems.∀

object S3DestinationSpec extends EffectfulQSpec[IO] {

  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  val Log = LoggerFactory.getLogger("S3DestinationSpec")

  val TestBucket = Bucket("fake-bucket")
  val MkPostfix = IO.delay("somepostfix")

  "create sink" >> {
    "duplicates the filename as a prefix" >>* {
      for {
        (upload, ref) <- MockUpload.empty
        testPath = ResourcePath.root() / ResourceName("foo") / ResourceName("bar.csv")
        bytes = Stream("foobar").through(text.utf8Encode)
        _ <- runCreateSink(upload, testPath, bytes)
        keys <- ref.get.map(_.keys)
      } yield {
        keys must contain(exactly(ObjectKey("foo/bar/bar.csv")))
      }
    }

    "adds csv extension to existing one" >>* {
      for {
        (upload, ref) <- MockUpload.empty
        testPath = ResourcePath.root() / ResourceName("foo") / ResourceName("bar.whatever")
        bytes = Stream("foobar").through(text.utf8Encode)
        _ <- runCreateSink(upload, testPath, bytes)
        keys <- ref.get.map(_.keys)
      } yield {
        keys must contain(exactly(ObjectKey("foo/bar/bar.whatever.csv")))
      }
    }

    "adds csv extenstion if there is no extenstion" >>* {
      for {
        (upload, ref) <- MockUpload.empty
        testPath = ResourcePath.root() / ResourceName("foo") / ResourceName("bar")
        bytes = Stream("foobar").through(text.utf8Encode)
        _ <- runCreateSink(upload, testPath, bytes)
        keys <- ref.get.map(_.keys)
      } yield {
        keys must contain(exactly(ObjectKey("foo/bar/bar.csv")))
      }
    }

    "rejects ResourcePath.root() with ResourceError.NotAResource" >>* {
      for {
        (upload, _) <- MockUpload.empty
        testPath = ResourcePath.root()
        bytes = Stream.empty
        res <- runCreateSink(upload, testPath, bytes).map(_.asRight[ResourceError]) recover {
          case ResourceError.throwableP(re) => re.asLeft[Unit]
        }
      } yield {
        res must beLeft.like {
          case ResourceError.NotAResource(path) => path must_== testPath
        }
      }
    }

    "uploads results" >>* {
      for {
        (upload, ref) <- MockUpload.empty
        testPath = ResourcePath.root() / ResourceName("foo") / ResourceName("bar.csv")
        bytes = Stream("push this").through(text.utf8Encode)
        _ <- runCreateSink(upload, testPath, bytes)
        currentStatus <- ref.get
      } yield {
        currentStatus.get(ObjectKey("foo/bar/bar.csv")) must beSome("push this")
      }
    }
  }

  val evs: Stream[IO, AppendEvent[Byte, OffsetKey.Actual[String]]] = Stream(
    DataEvent.Create(Chunk.array("push ".getBytes)),
    DataEvent.Create(Chunk.array("this".getBytes)),
    DataEvent.Commit(OffsetKey.Actual.string("commit0")))

  "append sink" >> {
    "duplicates the filename as a prefix and includes postfix" >>* {
      for {
        (upload, ref) <- MockUpload.empty
        testPath = ResourcePath.root() / ResourceName("foo") / ResourceName("bar.csv")
        offsets <- runAppendSink(upload, testPath, evs)
        keys <- ref.get.map(_.keys)
      } yield {
        offsets.size must be_===(1)
        keys must contain(exactly(ObjectKey("foo/bar/bar.somepostfix.csv")))
      }
    }

    "adds csv extension to existing one" >>* {
      for {
        (upload, ref) <- MockUpload.empty
        testPath = ResourcePath.root() / ResourceName("foo") / ResourceName("bar.whatever")
        offsets <- runAppendSink(upload, testPath, evs)
        keys <- ref.get.map(_.keys)
      } yield {
        offsets.size must be_===(1)
        keys must contain(exactly(ObjectKey("foo/bar/bar.somepostfix.whatever.csv")))
      }
    }

    "adds csv extenstion if there is no extenstion" >>* {
      for {
        (upload, ref) <- MockUpload.empty
        testPath = ResourcePath.root() / ResourceName("foo") / ResourceName("bar")
        offsets <- runAppendSink(upload, testPath, evs)
        keys <- ref.get.map(_.keys)
      } yield {
        keys must contain(exactly(ObjectKey("foo/bar/bar.somepostfix.csv")))
      }
    }

    "rejects ResourcePath.root() with ResourceError.NotAResource" >>* {
      for {
        (upload, _) <- MockUpload.empty
        testPath = ResourcePath.root()
        bytes = Stream.empty
        res <- runAppendSink(upload, testPath, evs).map(_.asRight[ResourceError]) recover {
          case ResourceError.throwableP(re) => re.asLeft[List[OffsetKey.Actual[String]]]
        }
      } yield {
        res must beLeft.like {
          case ResourceError.NotAResource(path) => path must_== testPath
        }
      }
    }

    "uploads results" >>* {
      for {
        (upload, ref) <- MockUpload.empty
        testPath = ResourcePath.root() / ResourceName("append") / ResourceName("bar.csv")
        offsets <- runAppendSink(upload, testPath, evs)
        currentStatus <- ref.get
      } yield {
        offsets.size must be_===(1)
        currentStatus.get(ObjectKey("append/bar/bar.somepostfix.csv")) must beSome("push this")
      }
    }
  }

  private def runCreateSink(upload: Upload[IO], path: ResourcePath, bytes: Stream[IO, Byte]): IO[Unit] = {
    findCreateSink(S3Destination[IO](Log, TestBucket, None, upload, MkPostfix).sinks).fold(
      IO.raiseError[Unit](new Exception("Could not find create sink in S3Destination"))
    )(_.consume(path, NonEmptyList.one(Column("test", ())))._2(bytes).compile.drain)
  }

  private def findCreateSink(sinks: NonEmptyList[ResultSink[IO, Unit]]): Option[ResultSink.CreateSink[IO, Unit, Byte]] =
    sinks collectFirstSome {
      case csvSink @ ResultSink.CreateSink(_) =>
        csvSink.asInstanceOf[ResultSink.CreateSink[IO, Unit, Byte]].some
      case _ => None
    }

  private def runAppendSink(upload: Upload[IO], path: ResourcePath, evs: Stream[IO, AppendEvent[Byte, OffsetKey.Actual[String]]]): IO[List[OffsetKey.Actual[String]]] = {
    findAppendSink(S3Destination[IO](Log, TestBucket, None, upload, MkPostfix).sinks).fold(
      IO.raiseError[List[OffsetKey.Actual[String]]](new Exception("Could not find append sink in S3Destination"))
    )(sink => consumeAppend(sink, path, evs))
  }

  type AppendPipe = ∀[S3Destination.Consume[IO, AppendEvent[Byte, *], *]]

  def consumeAppend(
      sink: ResultSink.AppendSink[IO, Byte],
      path: ResourcePath,
      bytes: Stream[IO, AppendEvent[Byte, OffsetKey.Actual[String]]])
      : IO[List[OffsetKey.Actual[String]]] = {
    val cols: PushColumns[Column[Byte]] = PushColumns.NoPrimary(NonEmptyList.one(Column("x", 1.toByte)))
    val appendPipe = sink.consume(ResultSink.AppendSink.Args[Byte](path, cols, WriteMode.Replace)).pipe.asInstanceOf[AppendPipe]
    appendPipe[String](bytes).compile.toList
  }

  private def findAppendSink(sinks: NonEmptyList[ResultSink[IO, Unit]]): Option[ResultSink.AppendSink[IO, Byte]] =
    sinks collectFirstSome {
      case csvSink @ ResultSink.AppendSink(_) =>
        csvSink.asInstanceOf[ResultSink.AppendSink[IO, Byte]].some
      case _ => None
    }

  private implicit val ioMonadResourceErr: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)
}

final class MockUpload(status: Ref[IO, Map[ObjectKey, String]]) extends Upload[IO] {
  def upload(bytes: Stream[IO, Byte], bucket: Bucket, path: BlobPath): IO[Unit] =
    for {
      data <- bytes.through(text.utf8Decode).foldMonoid.compile.lastOrError
      key = ObjectKey(path.path.map(_.value).intercalate("/"))
      _ <- status.update(_ + (key -> data))
    } yield ()
}

object MockUpload {
  def empty: IO[(Upload[IO], Ref[IO, Map[ObjectKey, String]])] =
   Ref.of[IO, Map[ObjectKey, String]](Map.empty[ObjectKey, String])
     .map(ref => (new MockUpload(ref), ref))
}
