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
import quasar.api.destination.{DestinationError, DestinationType}
import quasar.blobstore.s3.Bucket
import quasar.concurrent.unsafe._
import quasar.connector.ResourceError
import quasar.contrib.scalaz.MonadError_

import java.net.URI
import java.nio.file.FileSystems

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global

import argonaut.{Argonaut, Json, Parse}, Argonaut._
import cats.effect.{Blocker, IO, Resource, Timer}
import fs2.{io, text, Stream}
import scalaz.NonEmptyList

object S3DestinationModuleSpec extends EffectfulQSpec[IO] {
  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  val blocker = Blocker.unsafeCached("s3-destination-module-spec")

  val TestBucket = "https://slamdata-public-test.s3.amazonaws.com"
  val NonExistantBucket = "https://slamdata-public-test-does-not-exist.s3.amazonaws.com"

  "unapplyBucketUri" >> {
    "virtual host style" >> {
      S3DestinationModule.unapplyBucketUri(Json.jEmptyString)(BucketUri("https://some-bucket.s3-eu-west-1.amazonaws.com")) must
        beRight((None, Bucket("some-bucket")))
    }

    "path-style" >> {
      S3DestinationModule.unapplyBucketUri(Json.jEmptyString)(BucketUri("https://s3-eu-west-1.amazonaws.com/some-bucket")) must
        beRight((None, Bucket("some-bucket")))
    }

    "wasabi" >> {
      S3DestinationModule.unapplyBucketUri(Json.jEmptyString)(BucketUri("https://s3.wasabisys.com/some-bucket")) must
        beRight((Some(new URI("https://s3.wasabisys.com")), Bucket("some-bucket")))
    }

    "virtual host style non-AWS is unsupported" >> {
      S3DestinationModule.unapplyBucketUri(Json.jEmptyString)(BucketUri("https://some-bucket.s3-eu-west-1.amazonnotaws.com")) must
        beLeft
    }

    "invalid S3 endpoint" >> {
      S3DestinationModule.unapplyBucketUri(Json.jEmptyString)(BucketUri("https://something.com")) must
        beLeft
    }
  }

  "creates a destination with valid credentials" >>* {
    val destination =
      Resource.suspend(configWith(TestBucket).map(
        S3DestinationModule.destination[IO](_, _ => _ => Stream.empty, _ => IO.pure(None))))

    destination.use(dst => IO(dst must beRight))
  }

  "validates bucket exists" >>* {
    val destination =
      Resource.suspend(configWith(NonExistantBucket).map(
        S3DestinationModule.destination[IO](_, _ => _ => Stream.empty, _ => IO.pure(None))))

    destination.use(dst => IO(dst must beLeft.like {
      case DestinationError.InvalidConfiguration(dt, _, rs) =>
        dt must_== DestinationType("s3", 1L)
        rs must_== NonEmptyList("Bucket does not exist")
    }))
  }

  "validates credentials work" >>* {
    val config =
      readCredentials.map(creds =>
        Json.obj(
          "bucket" := TestBucket,
          "credentials" := invalidateCredentials(creds)))

    val destination =
      Resource.suspend(config.map(
        S3DestinationModule.destination[IO](_, _ => _ => Stream.empty, _ => IO.pure(None))))

    destination.use(dst => IO(dst must beLeft.like {
      case DestinationError.AccessDenied(dt, _, msg) =>
        dt must_== DestinationType("s3", 1L)
        msg must_== "Access denied"
    }))
  }

  def invalidateCredentials(c: Json): Json =
    c.withObject(_ + ("secretKey", "wrong-key".asJson))

  def configWith(bucket: String): IO[Json] =
    readCredentials.map(creds =>
      Json.obj(
        "bucket" := bucket,
        "credentials" := creds))

  def readCredentials: IO[Json] =
    io.file
      .readAll[IO](FileSystems.getDefault.getPath("testCredentials.json"), blocker, 4096)
      .through(text.utf8Decode).compile.string
      .flatMap(str =>
        Parse.parse(str).fold(_ => IO.raiseError(new Exception("Couldn't parse testCredentials.json")), IO(_)))

  implicit val ioMonadResourceErr: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)
}
