// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//

package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static io.github.simbo1905.no.framework.Companion.BOXED_PRIMITIVES;
import static io.github.simbo1905.no.framework.Companion.recordClassHierarchy;


/// Main interface for the No Framework Pickler serialization library.
/// Provides type-safe, reflection-free serialization for records and sealed interfaces.
public sealed interface Pickler<T> permits EmptyRecordPickler, ManyPickler, RecordPickler {

  Logger LOGGER = Logger.getLogger(Pickler.class.getName());

  /// Serialize an object to a ByteBuffer
  /// @param buffer The buffer to write to
  /// @param record The object to serialize
  /// @return The number of bytes written
  int serialize(ByteBuffer buffer, T record);

  /// Deserialize an object from a ByteBuffer
  /// @param buffer The buffer to read from
  /// @return The deserialized object
  T deserialize(ByteBuffer buffer);

  /// Calculate the maximum size needed to serialize an object
  /// @param record The object to size
  /// @return The maximum number of bytes needed
  int maxSizeOf(T record);

  /// Factory method for creating a unified pickler for any type
  /// @param clazz The root class (record, enum, or sealed interface)
  /// @return A pickler instance
  static <T> Pickler<T> forClass(Class<T> clazz) {
    Objects.requireNonNull(clazz, "Class must not be null");

    if (!clazz.isRecord() && !clazz.isEnum() && !clazz.isSealed()) {
      throw new IllegalArgumentException("Class must be a record, enum, or sealed interface: " + clazz);
    }

    final var recordClassHierarchy = recordClassHierarchy(clazz);

    // Partition the class hierarchy into legal and illegal classes
    final Map<Boolean, List<Class<?>>> legalAndIllegalClasses =
        recordClassHierarchy.stream().collect(Collectors.partitioningBy(
            cls -> cls.isRecord() || cls.isEnum() || cls.isSealed() || cls.isArray() || cls.isPrimitive()
                || String.class.equals(cls) || UUID.class.isAssignableFrom(cls) || List.class.isAssignableFrom(cls)
                || Optional.class.isAssignableFrom(cls)
                || Map.class.isAssignableFrom(cls) || BOXED_PRIMITIVES.contains(cls)
        ));

    final var illegalClasses = legalAndIllegalClasses.get(Boolean.FALSE);
    if (!illegalClasses.isEmpty()) {
      throw new IllegalArgumentException("All reachable must be a record, enum, or sealed interface of only records " +
          "and enum. Found illegal types: " +
          legalAndIllegalClasses.get(Boolean.FALSE).stream()
              .map(Class::getName).collect(Collectors.joining(", ")));
    }

    final var recordClasses = legalAndIllegalClasses.get(Boolean.TRUE).stream().filter(Class::isRecord).toList();
    if (recordClasses.isEmpty()) {
      throw new IllegalArgumentException("No record classes found in hierarchy of: " + clazz);
    }

    final var enumClasses = legalAndIllegalClasses.get(Boolean.TRUE).stream().filter(Class::isEnum).toList();

    final Map<Class<Enum<?>>, Long> enumToTypeSignatureMap = enumClasses.stream()
        .map(cls -> {
          @SuppressWarnings("unchecked") final Class<Enum<?>> cast = (Class<Enum<?>>) cls;
          return cast;
        })
        .filter(Enum.class::isAssignableFrom)
        .map(enumClass -> Map.entry(enumClass, Companion.hashSignature(enumClass.getName())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    if (recordClasses.size() == 1) {
      final var first = recordClasses.getFirst();
      final var components = first.getRecordComponents();
      if (components.length == 0) {
        // If the record has no components, we can return an EmptyRecordPickler
        LOGGER.fine(() -> "Creating EmptyRecordPickler for record class: " + first.getSimpleName());
        return new EmptyRecordPickler<>(first);
      } else {
        // If the record has components, we can return a RecordPickler
        LOGGER.fine(() -> "Creating RecordPickler for record class: " + first.getSimpleName());
        return new RecordPickler<>(first, enumToTypeSignatureMap);
      }
    } else {
      // If there are multiple record classes return a RecordPickler that will delegate to a RecordPickler
      LOGGER.fine(() -> "Creating PicklerRoot for multiple record classes: " +
          recordClasses.stream().map(Class::getSimpleName).collect(Collectors.joining(", ")));
      return new ManyPickler<>(recordClasses, enumToTypeSignatureMap);
    }
  }
}
