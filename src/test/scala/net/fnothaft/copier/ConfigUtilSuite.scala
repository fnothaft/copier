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

import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSuite

class ConfigUtilSuite extends FunSuite {

  test("round trip a configuration") {
    val config = new Configuration()

    config.set("a.string.value", "myString")
    config.setInt("an.int.value", 1234)
    config.setLong("a.long.value", 4321L)

    val configMap = ConfigUtil.extractConfig(config)
    val roundTripConfig = ConfigUtil.makeConfig(configMap)

    assert(roundTripConfig.get("a.string.value") === "myString")
    assert(roundTripConfig.getInt("an.int.value", 0) === 1234)
    assert(roundTripConfig.getLong("a.long.value", 0L) === 4321L)
  }
}
