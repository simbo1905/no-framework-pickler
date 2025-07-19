// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import java.util.Objects;

/// Handler for custom value-based types with serialization logic
record SerdeHandler(
    Class<?> valueBasedLike,
    int marker,
    Serde.Sizer sizer,
    Serde.Writer writer,
    Serde.Reader reader
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
