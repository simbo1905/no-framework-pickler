// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//

package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.Objects;

final class Serde<T> implements Pickler<T> {
  private final Serdes.Sizer sizer;
  private final Serdes.Writer writer;
  private final Serdes.Reader reader;

  Serde(Serdes.Sizer sizer, Serdes.Writer writer, Serdes.Reader reader) {
    this.sizer = Objects.requireNonNull(sizer);
    this.writer = Objects.requireNonNull(writer);
    this.reader = Objects.requireNonNull(reader);
  }

  @Override
  public int serialize(ByteBuffer buffer, T record) {
    final int start = buffer.position();
    writer.accept(buffer, record);
    return buffer.position() - start;
  }

  @Override
  public T deserialize(ByteBuffer buffer) {
    @SuppressWarnings("unchecked")
    T result = (T) reader.apply(buffer);
    return result;
  }

  @Override
  public int maxSizeOf(T record) {
    return sizer.applyAsInt(record);
  }

  @Override
  public long typeSignature(Class<?> originalClass) {
    throw new UnsupportedOperationException("Custom handlers do not have type signatures.");
  }
}
