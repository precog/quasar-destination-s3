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

import quasar.api.resource.{ResourceName, ResourcePath}
import quasar.blobstore.s3.{AccessKey, SecretKey, Region}

import argonaut.{Argonaut, DecodeJson, EncodeJson, Json}, Argonaut._
import scalaz.IList

final case class S3Credentials(accessKey: AccessKey, secretKey: SecretKey, region: Region)
final case class BucketUri(value: String)
final case class PrefixPath(value: String) {
  lazy val toResourcePath: ResourcePath =
    ResourcePath.resourceNamesIso(IList(value.split("/").filterNot(_.isEmpty).map(ResourceName(_)) :_*))
}
final case class S3Config(bucketUri: BucketUri, prefixPath: Option[PrefixPath], credentials: S3Credentials)

object S3Config {
  implicit val s3CredentialsDecodeJson: DecodeJson[S3Credentials] =
    DecodeJson(c => for {
      accessKey <- c.downField("accessKey").as[String]
      secretKey <- c.downField("secretKey").as[String]
      region <- c.downField("region").as[String]
    } yield S3Credentials(AccessKey(accessKey), SecretKey(secretKey), Region(region)))

  implicit val s3ConfigDecodeJson: DecodeJson[S3Config] =
    DecodeJson(c => for {
      uri <- c.downField("bucket").as[String]
      prefixPath <- c.downField("prefixPath").as[Option[String]]
      creds <- c.downField("credentials").as[S3Credentials]
    } yield S3Config(BucketUri(uri), prefixPath.map(PrefixPath(_)), creds))

  private implicit val s3CredentialsEncodeJson: EncodeJson[S3Credentials] =
    EncodeJson(creds => Json.obj(
      "accessKey" := creds.accessKey.value,
      "secretKey" := creds.secretKey.value,
      "region" := creds.region.value))

  implicit val s3ConfigEncodeJson: EncodeJson[S3Config] =
    EncodeJson(config =>
      ("prefixPath" :?= config.prefixPath.map(_.value)) ->?:
        ("bucket" := config.bucketUri.value) ->:
        ("credentials" := config.credentials.asJson) ->:
        jEmptyObject)

}
