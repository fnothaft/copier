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

import java.util.{ Map => JMap }
import org.apache.hadoop.conf.Configuration
import scala.collection.JavaConversions._

/**
 * Hadoop's Configuration class is not serializable so we go to a map and back.
 */
private[copier] object ConfigUtil extends Serializable {

  /**
   * Builds a Hadoop configuration from a Map of string/string pairs.
   *
   * @param config The Map of configuration key/value pairs.
   * @return Returns a Hadoop Configuration object corresponding to these pairs.
   */
  def makeConfig(config: Map[String, String]): Configuration = {
    val hadoopConfig = new Configuration()

    config.foreach(kv => {
      val (key, value) = kv
      hadoopConfig.set(key, value)
    })

    hadoopConfig
  }

  /**
   * Extracts a Map of string/string pairs from a Hadoop Configuration.
   *
   * @param config The Hadoop Configuration instance.
   * @return Returns the Map of configuration key/value pairs corresponding to
   *   this Hadoop Configuration object.
   */
  def extractConfig(config: Configuration): Map[String, String] = {
    val iter: Iterator[JMap.Entry[String, String]] = config.iterator()

    iter.map(e => {
      (e.getKey, e.getValue)
    }).toMap
  }
}
