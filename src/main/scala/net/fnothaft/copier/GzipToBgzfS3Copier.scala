/**
 * Copyright 2017 Frank Austin Nothaft
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
package net.fnothaft.copier

import com.amazonaws.auth._
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model._
import htsjdk.samtools.util.BlockCompressedOutputStream
import java.io.{ InputStream, OutputStream }
import java.net.{ ConnectException, URL, URLConnection }
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.utils.cli._
import org.bdgenomics.utils.misc.Logging
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }
import scala.collection.JavaConversions._

object GzipToBgzfS3Copier extends BDGCommandCompanion with Logging {

  val commandName = "gzipToBgzfS3Copier"
  val commandDescription = "Copy Gzip'ed filess to S3, BGZFing them along the way."

  def apply(cmdLine: Array[String]) = {
    log.info("Running Copier with arguments: Array(%s)".format(
      cmdLine.mkString(",")))

    new GzipToBgzfS3Copier(Args4j[GzipToBgzfS3CopierArgs](cmdLine))
  }

  /**
   * Copies a single path to S3
   *
   * Gunzips the file, BGZFs it, and uploads it to S3.
   *
   * @param file The path to download.
   * @param outputBucket The output directory to write the file to.
   * @param config The Hadoop Configuration values to use.
   * @return Returns the number of bytes written to disk.
   */
  private def copyFile(file: String,
                       outputBucket: String,
                       blockSize: Int,
                       bufferSize: Int,
                       config: Map[String, String]): Long = {

    // create our path objects
    val filename = file.split("/").last
    val inPath = new Path(file)

    // reconstruct the config and get the FS for the output path
    val conf = ConfigUtil.makeConfig(config)
    val inFs = inPath.getFileSystem(conf)

    // create our streams
    val is = inFs.open(inPath)
    val gzipIs = new GzipCodec().createInputStream(is)
    val buffer = new CircularBuffer(bufferSize)
    val bgzfOs = new BlockCompressedOutputStream(buffer.outputStream, null)

    // create the s3 upload
    val client = new AmazonS3Client(new ProfileCredentialsProvider())
    val request = new InitiateMultipartUploadRequest(outputBucket, filename)
    val uploadId = client.initiateMultipartUpload(request).getUploadId

    // loop and copy blocks
    val copyBuffer = new Array[Byte](bufferSize)
    var doneCopying = false
    var blockIdx = 0
    var tags = List[PartETag]()
    var bytesCopied = 0L
    while (!doneCopying) {
      while (buffer.entries < blockSize) {
        val bytesRead = gzipIs.read(copyBuffer)
        if (bytesRead != bufferSize) {
          doneCopying = true
        }
        bgzfOs.write(copyBuffer)
      }

      val part = new UploadPartRequest()
        .withBucketName(outputBucket)
        .withKey(filename)
        .withUploadId(uploadId)
        .withPartNumber(blockIdx)
        .withInputStream(buffer.inputStream)

      // TODO: submit part upload
      if (doneCopying) {

        val remainingBytes = buffer.entries
        tags = client.uploadPart(part.withPartSize(remainingBytes)).getPartETag :: tags

        log.info("Uploaded final part %d of %s/%s with size %d.".format(blockIdx,
          outputBucket,
          filename,
          remainingBytes))
        bytesCopied += remainingBytes
      } else {

        tags = client.uploadPart(part.withPartSize(blockSize)).getPartETag :: tags
        log.info("Uploaded part %d of %s/%s with size %d.".format(blockIdx,
          outputBucket,
          filename,
          blockSize))
        blockIdx += 1
        bytesCopied += blockSize
      }
    }

    client.completeMultipartUpload(new CompleteMultipartUploadRequest()
      .withBucketName(outputBucket)
      .withKey(filename)
      .withUploadId(uploadId)
      .withPartETags(tags))

    bytesCopied
  }

  /**
   * Copies all the URLs in an RDD of URLs to disk.
   *
   * @param rdd The RDD of URLs to download.
   * @param outputBucket The output directory to write the file to.
   * @param blockSize The S3 block size to use.
   * @param bufferSize The buffer size to use on the output stream.
   * @return Returns the total number of bytes written to disk across all files.
   */
  def copy(rdd: RDD[String],
           outputBucket: String,
           blockSize: Int,
           bufferSize: Int): Long = {

    // how many files do we have to download?
    // we will repartition into one per partition
    val files = rdd.count().toInt

    // excise the hadoop conf values
    val configMap = ConfigUtil.extractConfig(rdd.context.hadoopConfiguration)

    // repartition and run the downloads
    rdd.repartition(files)
      .map(copyFile(_, outputBucket,
        blockSize, bufferSize,
        configMap))
      .reduce(_ + _)
  }
}

class GzipToBgzfS3CopierArgs extends Args4jBase {
  @Argument(required = true,
    metaVar = "PATHS",
    usage = "The paths to download",
    index = 0)
  var inputPath: String = null
  @Argument(required = true,
    metaVar = "OUTPUT",
    usage = "Location to write the downloaded data",
    index = 1)
  var outputPath: String = null
  @Args4jOption(required = false,
    name = "-buffer_size",
    usage = "The size of the buffer for writing to the output directory. Defaults to 16Ki.")
  var bufferSize = 16 * 1024
  @Args4jOption(required = false,
    name = "-block_size",
    usage = "The size of the S3 block size for writing. Defaults to 64Mi.")
  var blockSize = 64 * 1024 * 1024
}

class GzipToBgzfS3Copier(
    protected val args: GzipToBgzfS3CopierArgs) extends BDGSparkCommand[GzipToBgzfS3CopierArgs] {
  val companion = GzipToBgzfS3Copier

  def run(sc: SparkContext) {

    val files = sc.textFile(args.inputPath)

    companion.copy(files,
      args.outputPath,
      args.blockSize,
      args.bufferSize)
  }
}
