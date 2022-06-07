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
import quasar.blobstore.s3.Bucket
import quasar.blobstore.paths.{BlobPath, PathElem}
import quasar.connector.{MonadResourceErr, ResourceError}
import quasar.connector.destination.{ResultSink, UntypedDestination}
import quasar.connector.render.RenderConfig
import quasar.contrib.pathy.AFile

import cats.data.NonEmptyList
import cats.effect.{Concurrent, ContextShift}

import cats.implicits._

import fs2.{Pipe, Stream}

import pathy.Path

final class S3Destination[F[_]: Concurrent: ContextShift: MonadResourceErr](
  bucket: Bucket, prefixPath: Option[PrefixPath], uploadImpl: Upload[F])
    extends UntypedDestination[F] {

  import S3Destination._

  def destinationType: DestinationType = DestinationType("s3", 1L)

  def sinks: NonEmptyList[ResultSink[F, Unit]] =
    NonEmptyList.one(csvSink)

  private def csvSink = ResultSink.create[F, Unit, Byte] { (path, _) =>
    val pipe: Pipe[F, Byte, Unit] =
      bytes => Stream.eval(for {
        afile <- ensureAbsFile(prefixPath, path)
        path = ResourcePath.fromPath(nestResourcePath(afile))
        key = resourcePathToBlobPath(path)
        _ <- uploadImpl.upload(bytes, bucket, key)
      } yield ())

    (RenderConfig.Csv(), pipe)
  }

  private def nestResourcePath(file: AFile): AFile = {
    val withoutExtension = Path.fileName(Path.renameFile(file, _.dropExtension)).value
    val withExtension = Path.fileName(file)
    val withMandatoryExtension = withExtension.changeExtension { ext =>
      if (ext === MandatoryExtension || ext === "") MandatoryExtension
      else s"$ext.$MandatoryExtension"
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
  private val MandatoryExtension = "csv"

  def apply[F[_]: Concurrent: ContextShift: MonadResourceErr](bucket: Bucket, prefixPath: Option[PrefixPath], upload: Upload[F])
      : S3Destination[F] =
    new S3Destination[F](bucket, prefixPath, upload)
}
