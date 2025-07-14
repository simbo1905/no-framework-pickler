package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.function.Function;

interface Reader extends
    Function<ByteBuffer, Object> {

  /// Read an object from the ByteBuffer
  default Object read(ByteBuffer buffer) {
    return apply(buffer);
  }
}
