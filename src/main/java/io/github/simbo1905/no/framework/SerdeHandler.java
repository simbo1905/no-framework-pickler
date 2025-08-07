// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import java.util.Objects;

/// Handler for custom value-based types with serialization logic.
///
/// This record encapsulates the serialization strategy for custom types that aren't
/// natively supported by the pickler. Each handler provides:
///
/// - The target class this handler supports
/// - A unique positive marker for wire format identification
/// - Size calculation, writing, and reading operations
///
/// ## Example Usage
///
/// ```java
/// // Register a custom handler for LocalDate
/// var handler = SerdeHandler.forClass(
///     LocalDate.class,
///     100,  // Custom marker (must be positive)
///     (date) -> 8,  // Always 8 bytes (epoch day as long)
///     (buffer, date) -> buffer.putLong(date.toEpochDay()),
///     (buffer) -> LocalDate.ofEpochDay(buffer.getLong())
/// );
/// ```
///
/// @param valueBasedLike the class this handler supports
/// @param marker unique positive integer for wire format identification
/// @param sizer calculates the serialized size of an instance
/// @param writer writes an instance to a ByteBuffer
/// @param reader reads an instance from a ByteBuffer
public record SerdeHandler(
    Class<?> valueBasedLike,
    int marker,
    Serdes.Sizer sizer,
    Serdes.Writer writer,
    Serdes.Reader reader
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

  /// Creates a SerdeHandler for the specified class with the given serialization functions.
  ///
  /// This factory method provides a type-safe way to create handlers for custom types.
  ///
  /// @param <T> the type of objects this handler will serialize
  /// @param clazz the class object for type T
  /// @param marker unique positive integer for wire format identification
  /// @param sizer function to calculate serialized size
  /// @param writer function to write instances to ByteBuffer
  /// @param reader function to read instances from ByteBuffer
  /// @return a new SerdeHandler configured for the specified type
  /// @throws NullPointerException if any parameter is null
  /// @throws IllegalArgumentException if marker is not positive
  static public <T> SerdeHandler forClass(Class<T> clazz, int marker,
                                          Serdes.Sizer sizer,
                                          Serdes.Writer writer,
                                          Serdes.Reader reader) {
    return new SerdeHandler(clazz, marker, sizer, writer, reader);
  }
}
