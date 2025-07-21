// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import java.util.Objects;

/// Component serialization bundle that will allow for custom value-like types to be serialized
record ComponentSerde(
    Serdes.Writer writer,
    Serdes.Reader reader,
    Serdes.Sizer sizer
) {
  public ComponentSerde {
    Objects.requireNonNull(writer, "writer must not be null");
    Objects.requireNonNull(reader, "reader must not be null");
    Objects.requireNonNull(sizer, "sizer must not be null");
  }
}
