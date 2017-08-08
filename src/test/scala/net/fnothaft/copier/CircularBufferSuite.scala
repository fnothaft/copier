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

import org.scalatest.FunSuite

class CircularBufferSuite extends FunSuite {

  test("write the full size of the buffer and read out") {

    val buffer = new CircularBuffer(5)
    assert(buffer.size === 5)
    assert(buffer.entries === 0)

    // write once
    buffer.outputStream.write("HELLO".getBytes())
    assert(buffer.entries === 5)

    // buffer is now full
    intercept[IllegalStateException] {
      buffer.outputStream.write("HELLO".getBytes())
    }

    // read from buffer
    val bytes = new Array[Byte](5)
    assert(buffer.inputStream.read(bytes) === 5)
    assert(buffer.entries === 0)
    assert(new String(bytes) === "HELLO")

    // buffer is now empty
    assert(buffer.inputStream.read(bytes) === -1)

    // write and read again
    buffer.outputStream.write("HELLO".getBytes())
    assert(buffer.entries === 5)
    assert(buffer.inputStream.read(bytes) === 5)
    assert(buffer.entries === 0)
    assert(new String(bytes) === "HELLO")

    // write and read again, but mark the stream this time
    buffer.outputStream.write("HELLO".getBytes())
    assert(buffer.entries === 5)
    assert(buffer.inputStream.markSupported())
    buffer.inputStream.mark(10)
    bytes.indices.foreach(i => bytes(i) = '0') // "zero" out bytes
    assert(buffer.inputStream.read(bytes, 0, 3) === 3)
    assert(buffer.entries === 5)
    assert(new String(bytes) === "HEL00")
    buffer.inputStream.reset()
    assert(buffer.entries === 5)
    bytes.indices.foreach(i => bytes(i) = '0') // "zero" out bytes
    assert(buffer.inputStream.read(bytes) === 5)
    assert(buffer.entries === 0)
    assert(new String(bytes) === "HELLO")
  }

  test("partially fill the buffer, read, and then write around") {

    val buffer = new CircularBuffer(5)
    assert(buffer.size === 5)
    assert(buffer.entries === 0)

    // write once
    buffer.outputStream.write("ALLO".getBytes())
    assert(buffer.entries === 4)

    // read from buffer
    var bytes = new Array[Byte](4)
    assert(buffer.inputStream.read(bytes) === 4)
    assert(buffer.entries === 0)
    assert(new String(bytes) === "ALLO")
    assert(buffer.inputStream.read(bytes) === -1)

    // write again, wraps around buffer
    buffer.outputStream.write("ALLO".getBytes())
    assert(buffer.entries === 4)

    // write one character
    buffer.outputStream.write("Z".getBytes())
    assert(buffer.entries === 5)

    // buffer is now full
    intercept[IllegalStateException] {
      buffer.outputStream.write("HELLO".getBytes())
    }

    // read from buffer
    assert(buffer.inputStream.markSupported())
    buffer.inputStream.mark(4)
    bytes = new Array[Byte](2)
    assert(buffer.inputStream.read(bytes) === 2)
    assert(buffer.entries === 5)
    assert(new String(bytes) === "AL")
    buffer.inputStream.reset()
    bytes = new Array[Byte](4)
    assert(buffer.inputStream.read(bytes) === 4)
    assert(buffer.entries === 1)
    assert(new String(bytes) === "ALLO")

    bytes = new Array[Byte](1)
    assert(buffer.inputStream.read(bytes) === 1)
    assert(buffer.entries === 0)
    assert(new String(bytes) === "Z")

    // mark should have expired
    intercept[IllegalStateException] {
      buffer.inputStream.reset()
    }
  }
}
