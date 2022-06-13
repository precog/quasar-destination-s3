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

import scala.{None, Some}

import quasar.blobstore.s3.{AccessKey, Region, SecretKey}

import argonaut.{Argonaut, DecodeJson, Json}, Argonaut._
import org.specs2.mutable.Specification

object S3ConfigSpec extends Specification {
  "decodes and encodes configs with a virtual host-style bucket" >> {
    val json = Json.obj(
      "bucket" := "https://some-bucket.s3-eu-west-1.amazonaws.com",
      "credentials" := Json.obj(
        "accessKey" := "key1",
        "secretKey" := "key2",
        "region" := "eu-west-1"))

    val cfg = S3Config(
      BucketUri("https://some-bucket.s3-eu-west-1.amazonaws.com"),
      None,
      S3Credentials(AccessKey("key1"), SecretKey("key2"), Region("eu-west-1")))

    DecodeJson.of[S3Config].decodeJson(json).toOption must beSome(cfg)

    cfg.asJson.as[S3Config].toOption must beSome(cfg)
  }

  "decodes and encodes configs with a path-style bucket" >> {
    val json = Json.obj(
      "bucket" := "https://s3-eu-west-1.amazonaws.com/some-bucket",
      "credentials" := Json.obj(
        "accessKey" := "key1",
        "secretKey" := "key2",
        "region" := "eu-west-1"))

    val cfg = S3Config(
      BucketUri("https://s3-eu-west-1.amazonaws.com/some-bucket"),
      None,
      S3Credentials(AccessKey("key1"), SecretKey("key2"), Region("eu-west-1")))

    DecodeJson.of[S3Config].decodeJson(json).toOption must beSome(cfg)

    cfg.asJson.as[S3Config].toOption must beSome(cfg)
  }

  "decodes and encodes configs with prefixPath and path-style bucket" >> {
    val json = Json.obj(
      "bucket" := "https://s3-eu-west-1.amazonaws.com/some-bucket",
      "prefixPath" := "some/path",
      "credentials" := Json.obj(
        "accessKey" := "key1",
        "secretKey" := "key2",
        "region" := "eu-west-1"))

    val cfg = S3Config(
      BucketUri("https://s3-eu-west-1.amazonaws.com/some-bucket"),
      Some(PrefixPath("some/path")),
      S3Credentials(AccessKey("key1"), SecretKey("key2"), Region("eu-west-1")))

    DecodeJson.of[S3Config].decodeJson(json).toOption must beSome(cfg)

    cfg.asJson.as[S3Config].toOption must beSome(cfg)
  }
}
