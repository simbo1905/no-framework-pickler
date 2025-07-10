package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;

/// Component serialization bundle
record ComponentSerde(
    BiConsumer<ByteBuffer, Object> writer,
    Function<ByteBuffer, Object> reader,
    ToIntFunction<Object> sizer
) {
  public ComponentSerde {
    Objects.requireNonNull(writer, "writer must not be null");
    Objects.requireNonNull(reader, "reader must not be null");
    Objects.requireNonNull(sizer, "sizer must not be null");
  }
}
