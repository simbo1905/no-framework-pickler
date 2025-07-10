package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;

/// Handler for custom value-based types with serialization logic
record SerdeHandler(
    Class<?> valueBasedLike,
    int marker,
    ToIntFunction<Object> sizer,
    BiConsumer<ByteBuffer, Object> writer,
    Function<ByteBuffer, Object> reader
) {
  public SerdeHandler {
    Objects.requireNonNull(valueBasedLike, "valueBasedLike must not be null");
    if (marker <= 0) {
      throw new IllegalArgumentException("Custom type markers must be positive, got: " + marker);
    }
    Objects.requireNonNull(sizer, "sizer must not be null");
    Objects.requireNonNull(writer, "writer must not be null");
    Objects.requireNonNull(reader, "reader must not be null");
  }
}
