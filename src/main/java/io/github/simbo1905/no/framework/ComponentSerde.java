package io.github.simbo1905.no.framework;

import java.util.Objects;

/// Component serialization bundle
@SuppressWarnings("auxiliary-class")
record ComponentSerde(
    Writer writer,
    Reader reader,
    Sizer sizer
) {
  public ComponentSerde {
    Objects.requireNonNull(writer, "writer must not be null");
    Objects.requireNonNull(reader, "reader must not be null");
    Objects.requireNonNull(sizer, "sizer must not be null");
  }
}
