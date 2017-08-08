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

import java.io.{ InputStream, OutputStream }
import java.net.{ ConnectException, URL, URLConnection }
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.utils.cli._
import org.bdgenomics.utils.misc.Logging
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object Copier extends BDGCommandCompanion with Logging {

  val commandName = "copier"
  val commandDescription = "Copy URLs to HDFS."

  def apply(cmdLine: Array[String]) = {
    log.info("Running Copier with arguments: Array(%s)".format(
      cmdLine.mkString(",")))

    new Copier(Args4j[CopierArgs](cmdLine))
  }

  // for now, these are hard coded
  val retries = 4
  val sleepTime = 60

  /**
   * Opens a connection to the resource described by a URL.
   *
   * @param url The URL to connect to.
   * @param retriesLeft The number of retries to allow.
   * @return Returns an open connection to the resource.
   *
   * @throws ConnectException if the URL could not be connected to.
   */
  private def openConnection(url: URL,
                             retriesLeft: Int = retries): URLConnection = {
    try {
      url.openConnection()
    } catch {
      case ce: ConnectException => {
        if (retriesLeft >= 0) {
          log.warn("Connecting to %s failed. %d retries left.".format(url, retriesLeft - 1))
          log.warn("Sleeping %d seconds before retrying...".format(sleepTime))
          Thread.sleep(sleepTime)
          openConnection(url, retriesLeft - 1)
        } else {
          throw ce
        }
      }
    }
  }

  /**
   * Copies a single URL to disk.
   *
   * @param file The URL path to download.
   * @param outputDirectory The output directory to write the file to.
   * @param appendToFiles If true, appends to an existing file.
   * @param overwriteFiles If true, overwrites existing files.
   * @param blockSize The HDFS block size to use.
   * @param replication The HDFS replication factor to use.
   * @param bufferSize The buffer size to use on the output stream.
   * @param config The Hadoop Configuration values to use.
   * @return Returns the number of bytes written to disk.
   */
  private def copyFile(file: String,
                       outputDirectory: String,
                       appendToFiles: Boolean,
                       overwriteFiles: Boolean,
                       blockSize: Int,
                       replication: Int,
                       bufferSize: Int,
                       config: Map[String, String]): Long = {

    // create our path objects
    val filename = file.split("/").last
    val url = new URL(file)
    val outPath = new Path(outputDirectory, filename)

    // open a connection to the URL
    val cxn = openConnection(url)

    // reconstruct the config and get the FS for the output path
    val conf = ConfigUtil.makeConfig(config)
    val outFs = outPath.getFileSystem(conf)

    // create our streams
    val is = cxn.getInputStream()

    if (appendToFiles) {
      if (outFs.exists(outPath)) {
        // how many bytes are to be downloaded, and how many have we written?
        val bytesInResource = cxn.getContentLengthLong
        val bytesWritten = outFs.getFileStatus(outPath).getLen

        if (bytesInResource > bytesWritten) {
          log.info("Have already downloaded %d out of %d bytes to file %s. Resuming...".format(bytesWritten, bytesInResource, outPath))

          // open file in append mode
          val os = outFs.append(outPath, bufferSize)

          // copy the file
          doCopy(is, os, outPath)
        } else {
          log.info("File %s appears to have already been completely downloaded. Skipping...".format(url))
          0L
        }
      } else {
        log.info(
          "In append mode, but file %s did not exist. Downloading whole file.".format(
            outPath))
        downloadWholeFile(is,
          outFs, outPath,
          overwriteFiles,
          bufferSize, replication, blockSize)
      }
    } else {
      downloadWholeFile(is,
        outFs, outPath,
        overwriteFiles,
        bufferSize, replication, blockSize)
    }
  }

  /**
   * Downloads the entirety of a file.
   *
   * @param is The stream to the file.
   * @param outFs The file system to write to.
   * @param outPath The path to write the file to.
   * @param overwriteFiles If true, overwrites existing files.
   * @param bufferSize The buffer size to use on the output stream.
   * @param replication The HDFS replication factor to use.
   * @param blockSize The HDFS block size to use.
   * @return Returns the number of bytes written to disk.
   */
  private def downloadWholeFile(is: InputStream,
                                outFs: FileSystem,
                                outPath: Path,
                                overwriteFiles: Boolean,
                                bufferSize: Int,
                                replication: Int,
                                blockSize: Int): Long = {

    val os = outFs.create(outPath,
      overwriteFiles, bufferSize, replication.toShort, blockSize)

    doCopy(is, os, outPath)
  }

  /**
   * Copies all the bytes left in a stream to another stream.
   *
   * @param is The stream to copy from.
   * @param os The stream to copy to.
   * @param outPath The path to write the file to.
   * @return Returns the number of bytes written to disk.
   */
  private def doCopy(is: InputStream,
                     os: OutputStream,
                     outPath: Path): Long = {

    try {
      // copy to and fro
      val bytesCopied = IOUtils.copy(is, os)

      log.info("Successfully wrote %d bytes to %s.".format(bytesCopied, outPath))

      bytesCopied.toLong
    } finally {
      is.close()
      os.close()
    }
  }

  /**
   * Copies all the URLs in an RDD of URLs to disk.
   *
   * @param rdd The RDD of URLs to download.
   * @param outputDirectory The output directory to write the file to.
   * @param appendToFiles If true, appends to an existing file.
   * @param overwriteFiles If true, overwrites existing files.
   * @param blockSize The HDFS block size to use.
   * @param replication The HDFS replication factor to use.
   * @param bufferSize The buffer size to use on the output stream.
   * @return Returns the total number of bytes written to disk across all files.
   */
  def copy(rdd: RDD[String],
           outputDirectory: String,
           appendToFiles: Boolean,
           overwriteFiles: Boolean,
           blockSize: Int,
           replication: Int,
           bufferSize: Int): Long = {

    // how many files do we have to download?
    // we will repartition into one per partition
    val files = rdd.count().toInt

    // excise the hadoop conf values
    val configMap = ConfigUtil.extractConfig(rdd.context.hadoopConfiguration)

    // repartition and run the downloads
    rdd.repartition(files)
      .map(copyFile(_, outputDirectory, appendToFiles, overwriteFiles,
        blockSize, replication, bufferSize,
        configMap))
      .reduce(_ + _)
  }
}

class CopierArgs extends Args4jBase {
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
    usage = "The size of the HDFS block size for writing. Defaults to 64Mi.")
  var blockSize = 64 * 1024 * 1024
  @Args4jOption(required = false,
    name = "-replication",
    usage = "The replication factor to set per block. Defaults to 2.")
  var replication = 2
  @Args4jOption(required = false,
    name = "-overwrite",
    usage = "If true, overwrites files that already exist on disk.")
  var overwrite = false
  @Args4jOption(required = false,
    name = "-append",
    usage = "If true, appends to files that already exist on disk.")
  var append = false
}

class Copier(protected val args: CopierArgs) extends BDGSparkCommand[CopierArgs] {
  val companion = Copier

  def run(sc: SparkContext) {

    require(!(args.append && args.overwrite),
      "Cannot set both -append and -overwrite.")

    val files = sc.textFile(args.inputPath)

    companion.copy(files,
      args.outputPath,
      args.overwrite,
      args.append,
      args.blockSize,
      args.replication,
      args.bufferSize)
  }
}
