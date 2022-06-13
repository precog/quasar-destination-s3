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
import quasar.connector.destination.{ResultSink, UntypedDestination, WriteMode}
import quasar.connector.render.RenderConfig
import quasar.contrib.pathy.AFile

import cats.data.NonEmptyList
import cats.effect.{Concurrent, ContextShift}

import cats.syntax.all._

import fs2.{Pipe, Stream}

import org.slf4s.Logger
import pathy.Path
import scalaz.syntax.show._
import skolems.∀

final class S3Destination[F[_]: ContextShift: MonadResourceErr](
  logger: Logger,
  bucket: Bucket,
  prefixPath: Option[PrefixPath],
  uploadImpl: Upload[F],
  mkPostfix: F[String])(
  implicit F: Concurrent[F])
    extends UntypedDestination[F] {

  import S3Destination._

  def destinationType: DestinationType = DestinationType("s3", 1L)

  def sinks: NonEmptyList[ResultSink[F, Unit]] =
    NonEmptyList.of(csvCreateSink, csvAppendSink, csvUpsertSink)

  private def csvCreateSink = ResultSink.create[F, Unit, Byte] { (path, _) =>
    (RenderConfig.Csv(), consume("CreateSink", path, F.pure(none[String])))
  }

  private def consume(logPrefix: String, path: ResourcePath, mkFullPostfix: F[Option[String]]): Pipe[F, Byte, Unit] = {
    bytes => Stream.eval(for {
      afile <- ensureAbsFile(prefixPath, path)
      postfix <- mkFullPostfix
      path = ResourcePath.fromPath(nestResourcePath(afile, postfix))
      key = resourcePathToBlobPath(path)
      _ <- F.delay(logger.debug(s"[$logPrefix] Uploading ${path.show} to ${key.path.map(_.value).intercalate("/")}"))
      _ <- uploadImpl.upload(bytes, bucket, key)
    } yield ())
  }

  private def csvAppendSink = ResultSink.append[F, Unit, Byte](append)

  private def append(appendArgs: ResultSink.AppendSink.Args[Unit])
      : (RenderConfig[Byte], ∀[Consume[F, AppendEvent[Byte, *], *]]) = {
    val c = ∀[Consume[F, AppendEvent[Byte, *], *]](consumePipe("AppendSink", appendArgs.path, appendArgs.writeMode))
    (RenderConfig.Csv(), c)
  }

  // Currently we simply log a warning upon receiving a `DataEvent.Delete` event.
  // In case of `WriteMode.Replace` the user can interpret the contents of the written file
  // as the result of a full load. This case can be recognized by the filename containing '.full'.
  // However in case of `WriteMode.Append` the written data would need to include the deleted id's too in order
  // to be able to interpret it correctly. This case can be recognized by the filename containing '.incremental'.
  private def csvUpsertSink = ResultSink.upsert[F, Unit, Byte](upsert)

  private def upsert(upsertArgs: ResultSink.UpsertSink.Args[Unit])
      : (RenderConfig[Byte], ∀[Consume[F, DataEvent[Byte, *], *]]) = {
    val c = ∀[Consume[F, DataEvent[Byte, *], *]](consumePipe("UpsertSink", upsertArgs.path, upsertArgs.writeMode))
    (RenderConfig.Csv(), c)
  }

  private def consumePipe[A](logPrefix: String, path: ResourcePath, writeMode: WriteMode)
      : Pipe[F, DataEvent[Byte, OffsetKey.Actual[A]], OffsetKey.Actual[A]] = {
    val typePostfix = writeMode match {
      case WriteMode.Append => "incremental"
      case WriteMode.Replace => "full"
    }
    val mkFullPostFix = mkPostfix.map(pf => s"${pf}.${typePostfix}".some)
    DataEventConsumer[F, A, Byte](logger, consume(logPrefix, path, mkFullPostFix))
  }

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
      logger: Logger, bucket: Bucket, prefixPath: Option[PrefixPath], upload: Upload[F], mkPostfix: F[String])
      : S3Destination[F] =
    new S3Destination[F](logger, bucket, prefixPath, upload, mkPostfix)
}
