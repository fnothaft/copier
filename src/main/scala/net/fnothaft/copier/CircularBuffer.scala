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

class CircularBuffer(bufferSize: Int) {

  val buffer = new Array[Byte](bufferSize)
  var start = 0
  var end = 0
  var isEmpty = true

  def entries: Int = {
    if (isEmpty) {
      0
    } else if (end == start) {
      size
    } else if (end > start) {
      end - start
    } else {
      (end + bufferSize) - start
    }
  }

  def size: Int = buffer.size

  val inputStream: InputStream = new CircularBufferInputStream(this)
  val outputStream: OutputStream = new CircularBufferOutputStream(this)
}

case class CircularBufferInputStream private[copier] (
    buffer: CircularBuffer) extends InputStream {

  var pos: Int = buffer.start
  var optMarkPos: Option[Int] = None
  var limit: Int = -1

  override def available(): Int = {
    buffer.entries
  }

  override def close() {
    // no-op
  }

  override def mark(readlimit: Int) {
    optMarkPos = Some(pos)
    limit = pos + readlimit
    if (limit > buffer.size) {
      limit -= buffer.size
    }
  }

  override def markSupported(): Boolean = {
    true
  }

  def read(): Int = {
    if (buffer.isEmpty) {
      -1
    } else {

      val byteRead = buffer.buffer(pos)
      pos += 1
      if (pos >= buffer.size) {
        pos = 0
      }
      if (pos == buffer.end) {
        buffer.isEmpty = true
      }

      optMarkPos match {
        case Some(markPos) => {
          if ((limit > markPos && pos >= limit) ||
            (limit < markPos && pos >= limit && pos < markPos)) {
            optMarkPos = None
            buffer.start = pos
          }
        }
        case None => {
          buffer.start += 1
          if (buffer.start == buffer.size) {
            buffer.start = 0
          }
        }
      }

      byteRead
    }
  }

  override def reset() {
    optMarkPos match {
      case Some(markPos) => {
        buffer.isEmpty = false
        pos = markPos
      }
      case None => {
        throw new IllegalStateException("Stream was not marked.")
      }
    }
  }

  override def skip(n: Long): Long = {
    val bytes = if (pos > buffer.end) {
      buffer.end + buffer.size - pos
    } else {
      buffer.end - pos
    }
    val toSkip = if (n > bytes) {
      n.toInt
    } else {
      bytes
    }
    pos += toSkip
    toSkip
  }
}

case class CircularBufferOutputStream private[copier] (
    buffer: CircularBuffer) extends OutputStream {

  override def close() {
    // no-op
  }

  override def flush() {
    // no-op
  }

  def write(b: Int) {
    if ((buffer.start == 0 && buffer.end == buffer.size) ||
      (buffer.start == buffer.end && !buffer.isEmpty)) {
      throw new IllegalStateException("Buffer is full.")
    }
    buffer.isEmpty = false
    buffer.buffer(buffer.end) = b.toByte
    buffer.end += 1
    if (buffer.end == buffer.size) {
      buffer.end = 0
    }
  }
}
