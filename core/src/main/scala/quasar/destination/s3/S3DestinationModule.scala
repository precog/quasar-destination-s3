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

import java.net.URI

import quasar.api.destination.DestinationError
import quasar.api.destination.DestinationError.InitializationError
import quasar.api.destination.DestinationType
import quasar.blobstore.BlobstoreStatus
import quasar.blobstore.s3.{AccessKey, Bucket, Region, SecretKey, S3StatusService}
import quasar.connector.{GetAuth, MonadResourceErr}
import quasar.connector.destination.{Destination, DestinationModule, PushmiPullyu}
import quasar.destination.s3.impl.DefaultUpload

import scala.util.Either

import argonaut.{Argonaut, Json}, Argonaut._
import com.amazonaws.services.s3.AmazonS3URI
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.regions.{Region => AwsRegion}
import cats.data.EitherT
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Resource, Timer}
import cats.implicits._
import scalaz.NonEmptyList

object S3DestinationModule extends DestinationModule {
  // Minimum 10MiB multipart uploads
  private val PartSize = 10 * 1024 * 1024
  private val Redacted = "<REDACTED>"
  private val RedactedCreds =
    S3Credentials(AccessKey(Redacted), SecretKey(Redacted), Region(Redacted))

  def destinationType = DestinationType("s3", 1L)

  def sanitizeDestinationConfig(config: Json) = config.as[S3Config].result match {
    case Left(_) =>
      Json.jEmptyObject // don't expose credentials, even if we fail to decode the configuration.
    case Right(cfg) => cfg.copy(credentials = RedactedCreds).asJson
  }

  def unapplyBucketUri(config: Json)(bucketUri: BucketUri): Either[InitializationError[Json], (Option[URI], Bucket)] =
    (for {
      uri <- Try(new AmazonS3URI(bucketUri.value)).toOption
      bucket <- Option(uri.getBucket)
      endpoint <-
        if (bucketUri.value.contains("amazonaws.com")) Some(None)
        else
          if (uri.isPathStyle) Some(Some(new URI(bucketUri.value.stripSuffix(bucket.value).stripSuffix("/"))))
          else None
    } yield (endpoint, Bucket(bucket))).toRight(DestinationError.malformedConfiguration((destinationType, config, s"Could not obtain bucket from bucket URI ${bucketUri.value}")))

  def destination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
      config: Json,
      pushPull: PushmiPullyu[F],
      auth: GetAuth[F]): Resource[F, Either[InitializationError[Json], Destination[F]]] = {

    val configOrError = config.as[S3Config].toEither.leftMap {
      case (err, _) =>
        DestinationError.malformedConfiguration((destinationType, config, err))
    }

    val sanitizedConfig = sanitizeDestinationConfig(config)

    (for {
      cfg <- EitherT(Resource.pure[F, Either[InitializationError[Json], S3Config]](configOrError))
      (endpoint, bucket) <- EitherT(Resource.pure[F, Either[InitializationError[Json], (Option[URI], Bucket)]](unapplyBucketUri(config)(cfg.bucketUri)))
      client <- EitherT(mkClient(cfg, endpoint).map(_.asRight[InitializationError[Json]]))
      upload = DefaultUpload(client, PartSize)
      _ <- EitherT(Resource.eval(isLive(client, sanitizedConfig, bucket)))
    } yield (S3Destination(bucket, cfg.prefixPath, upload): Destination[F])).value
  }

  private def isLive[F[_]: Concurrent: ContextShift](
    client: S3AsyncClient,
    originalConfig: Json,
    bucket: Bucket): F[Either[InitializationError[Json], Unit]] =
    S3StatusService(client, bucket) map {
      case BlobstoreStatus.Ok =>
        ().asRight
      case BlobstoreStatus.NotFound =>
        DestinationError
          .invalidConfiguration(
            (destinationType, originalConfig, NonEmptyList("Bucket does not exist")))
          .asLeft
      case BlobstoreStatus.NoAccess =>
        DestinationError.accessDenied(
          (destinationType, originalConfig, "Access denied"))
          .asLeft
      case BlobstoreStatus.NotOk(msg) =>
        DestinationError
          .invalidConfiguration(
            (destinationType, originalConfig, NonEmptyList(msg)))
          .asLeft
    }

  private def mkClient[F[_]: Concurrent](cfg: S3Config, endpoint: Option[URI]): Resource[F, S3AsyncClient] = {
    val client =
      Concurrent[F].delay {
        val builder =
          S3AsyncClient.builder
            .credentialsProvider(StaticCredentialsProvider.create(
              AwsBasicCredentials
                .create(
                  cfg.credentials.accessKey.value,
                  cfg.credentials.secretKey.value)))
            .region(AwsRegion.of(cfg.credentials.region.value))

        endpoint
          .map(builder.endpointOverride)
          .getOrElse(builder)
          .build
      }

    Resource.fromAutoCloseable[F, S3AsyncClient](client)
  }
}
