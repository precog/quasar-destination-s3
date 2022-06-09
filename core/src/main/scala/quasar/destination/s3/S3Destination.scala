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

import quasar.api.destination.DestinationType
import quasar.api.resource.ResourcePath
import quasar.api.push.OffsetKey
import quasar.blobstore.s3.Bucket
import quasar.blobstore.paths.{BlobPath, PathElem}
import quasar.connector.{AppendEvent, DataEvent, MonadResourceErr, ResourceError}
import quasar.connector.destination.{ResultSink, UntypedDestination}
import quasar.connector.render.RenderConfig
import quasar.contrib.pathy.AFile

import cats.data.NonEmptyList
import cats.effect.{Concurrent, ContextShift}

import cats.implicits._

import fs2.{Pipe, Stream}

import pathy.Path
import skolems.∀

final class S3Destination[F[_]: ContextShift: MonadResourceErr](
  bucket: Bucket, prefixPath: Option[PrefixPath], uploadImpl: Upload[F], mkPostfix: F[String])(implicit F: Concurrent[F])
    extends UntypedDestination[F] {

  import S3Destination._

  def destinationType: DestinationType = DestinationType("s3", 1L)

  def sinks: NonEmptyList[ResultSink[F, Unit]] =
    NonEmptyList.of(csvCreateSink, csvAppendSink, csvUpsertSink)

  private def csvCreateSink = ResultSink.create[F, Unit, Byte] { (path, _) =>
    (RenderConfig.Csv(), consume(path, addTimestamp = false))
  }

  private def consume(path: ResourcePath, addTimestamp: Boolean): Pipe[F, Byte, Unit] = {
    bytes => Stream.eval(for {
      afile <- ensureAbsFile(prefixPath, path)
      postfix <- if (addTimestamp) mkPostfix.map(_.some) else F.pure(none[String])
      path = ResourcePath.fromPath(nestResourcePath(afile, postfix))
      key = resourcePathToBlobPath(path)
      _ <- uploadImpl.upload(bytes, bucket, key)
    } yield ())
  }

  private def csvAppendSink = ResultSink.append[F, Unit, Byte](append)

  private def append(appendArgs: ResultSink.AppendSink.Args[Unit])
      : (RenderConfig[Byte], ∀[Consume[F, AppendEvent[Byte, *], *]]) = {
    val c = ∀[Consume[F, AppendEvent[Byte, *], *]](consumePipe(appendArgs.path))
    (RenderConfig.Csv(), c)
  }

  private def csvUpsertSink = ResultSink.upsert[F, Unit, Byte](upsert)

  private def upsert(upsertArgs: ResultSink.UpsertSink.Args[Unit])
      : (RenderConfig[Byte], ∀[Consume[F, DataEvent[Byte, *], *]]) = {
    val c = ∀[Consume[F, DataEvent[Byte, *], *]](consumePipe(upsertArgs.path))
    (RenderConfig.Csv(), c)
  }

  private def consumePipe[A](path: ResourcePath)
      : Pipe[F, DataEvent[Byte, OffsetKey.Actual[A]], OffsetKey.Actual[A]] =
    DataEventConsumer[F, A, Byte](consume(path, addTimestamp = true))

  private def nestResourcePath(file: AFile, postfix: Option[String]): AFile = {
    val withoutExtension = Path.fileName(Path.renameFile(file, _.dropExtension)).value
    val withExtension = Path.fileName(file)
    val withMandatoryExtension = withExtension.changeExtension { ext =>
      val ext0 =
        if (ext === MandatoryExtension || ext === "") MandatoryExtension
        else s"$ext.$MandatoryExtension"
      postfix.map(pf => s"$pf.$ext0").getOrElse(ext0)
    }
    val parent = Path.fileParent(file)
    parent </> Path.dir(withoutExtension) </> Path.file1(withMandatoryExtension)
  }

  private def resourcePathToBlobPath(rp: ResourcePath): BlobPath =
    BlobPath(
      ResourcePath.resourceNamesIso
        .get(rp)
        .map(rn => PathElem(rn.value)).toList)

  private def ensureAbsFile(prefixPath: Option[PrefixPath], r: ResourcePath): F[AFile] = {
    val rp = prefixPath.map(_.toResourcePath).getOrElse(ResourcePath.Root) ++ r
    rp.fold(_.pure[F], MonadResourceErr[F].raiseError(ResourceError.notAResource(r)))
  }
}

object S3Destination {

  type Consume[F[_], Event[_], A] =
    Pipe[F, Event[OffsetKey.Actual[A]], OffsetKey.Actual[A]]

  private val MandatoryExtension = "csv"

  def apply[F[_]: Concurrent: ContextShift: MonadResourceErr](
      bucket: Bucket, prefixPath: Option[PrefixPath], upload: Upload[F], mkPostfix: F[String])
      : S3Destination[F] =
    new S3Destination[F](bucket, prefixPath, upload, mkPostfix)
}
