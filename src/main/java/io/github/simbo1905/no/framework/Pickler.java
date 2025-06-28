// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//

package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static io.github.simbo1905.no.framework.Companion.recordClassHierarchy;


/// Main interface for the No Framework Pickler serialization library.
/// Provides type-safe, reflection-free serialization for records and sealed interfaces.
public sealed interface Pickler<T> permits EmptyRecordPickler, PicklerRoot, RecordPickler {

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

    final Set<Class<?>> BOXED_PRIMITIVES = Set.of(
        Byte.class, Short.class, Integer.class, Long.class,
        Float.class, Double.class, Character.class, Boolean.class
    );

    final var recordClassHierarchy = recordClassHierarchy(clazz);

    // Partition the class hierarchy into legal and illegal classes
    final Map<Boolean, List<Class<?>>> legalAndIllegalClasses =
        recordClassHierarchy.collect(Collectors.partitioningBy(
            cls -> cls.isRecord() || cls.isEnum() || cls.isSealed() || cls.isArray() || cls.isPrimitive()
                || String.class.equals(cls) || UUID.class.isAssignableFrom(cls)
                || BOXED_PRIMITIVES.contains(cls)
        ));

    final var illegalClasses = legalAndIllegalClasses.get(Boolean.FALSE);
    if (!illegalClasses.isEmpty()) {
      throw new IllegalArgumentException("All reachable must be a record, enum, or sealed interface of only records " +
          "and enum. Found illegal types: " +
          legalAndIllegalClasses.get(Boolean.FALSE).stream()
              .map(Class::getName).collect(Collectors.joining(", ")));
    }

    final var legalClasses = legalAndIllegalClasses.get(Boolean.TRUE);
    if (legalClasses.isEmpty()) {
      throw new IllegalArgumentException("No  classes of type record or enum found in hierarchy of: " + clazz);
    }

    // Partition the legal classes into records and enums
    final var recordsAndEnums = legalClasses.stream()
        .filter(claz -> claz.isRecord() || claz.isEnum())
        .collect(Collectors.partitioningBy(Class::isRecord));

    // if there are no records then we have no work to do
    final var recordClasses = recordsAndEnums.get(Boolean.TRUE);
    if (recordClasses.isEmpty()) {
      throw new IllegalArgumentException("No record classes found in hierarchy of: " + clazz);
    }

    if (recordClasses.size() == 1) {
      // If there is only one record class, we can return a RecordPickler
      LOGGER.info("Creating RecordPickler for single record class: " + recordClasses.getFirst().getSimpleName());
      return new RecordPickler<>(recordClasses.getFirst());
    } else {
      // If there are multiple record classes return a RecordPickler that will delegate to a RecordPickler
      LOGGER.info("Creating PicklerRoot for multiple record classes: " +
          recordClasses.stream().map(Class::getSimpleName).collect(Collectors.joining(", ")));
      return new PicklerRoot<>(recordClasses);
    }
  }
}
