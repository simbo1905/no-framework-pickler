// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;

sealed interface Serde permits Serde.Nothing {
  enum Nothing implements Serde {}

  interface Writer extends
      BiConsumer<ByteBuffer, Object> {

    /// Write an object to the ByteBuffer
    default void write(ByteBuffer buffer, Object obj) {
      accept(buffer, obj);
    }
  }

  interface WriterResolver extends
      Function<Class<?>, Writer> {

  }

  interface SizerResolver extends
      Function<Class<?>, Sizer> {
  }

  interface Sizer extends ToIntFunction<Object> {
    /// Get the size of an object of the given class
    default int sizeOf(Object obj) {
      return applyAsInt(obj);
    }
  }

  interface Reader extends
      Function<ByteBuffer, Object> {

    /// Read an object from the ByteBuffer
    default Object read(ByteBuffer buffer) {
      return apply(buffer);
    }
  }

  interface ReaderResolver extends
      Function<Long, Reader> {
  }
}
