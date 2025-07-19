// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.function.BiConsumer;

interface Writer extends
    BiConsumer<ByteBuffer, Object> {

  /// Write an object to the ByteBuffer
  default void write(ByteBuffer buffer, Object obj) {
    accept(buffer, obj);
  }

  interface Resolver {
  }
}
