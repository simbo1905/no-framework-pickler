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
public sealed interface Pickler<T> permits EmptyRecordSerde, PicklerImpl, RecordSerde {

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

  /// Factory method for creating a unified pickler for any type using refactored architecture
  /// @param clazz The root class (record, enum, or sealed interface)
  /// @return A pickler instance
  static <T> Pickler<T> forClass(Class<T> clazz) {
    return forClass(clazz, Map.of());
  }

  /// Factory method for creating a unified pickler for any type using refactored architecture
  /// @param clazz The root class (record, enum, or sealed interface)
  /// @param typeSignatures A map of class signatures to type signatures for backwards compatibility
  /// @return A pickler instance
  static <T> Pickler<T> forClass(Class<T> clazz, Map<Class<?>, Long> typeSignatures) {
    Objects.requireNonNull(clazz, "Class must not be null");
    if (!clazz.isRecord() && !clazz.isEnum() && !clazz.isSealed()) {
      throw new IllegalArgumentException("Class must be a record, enum, or sealed interface: " + clazz);
    }
    Objects.requireNonNull(typeSignatures, "Type signatures map must not be null");

    // Validate type signatures map as it is user input.
    typeSignatures.keySet().stream().filter(k -> k == null || !k.isRecord())
        .findAny()
        .ifPresent(k -> {
          throw new IllegalArgumentException("Type signatures map must not contain null keys " +
              "or a class that is not a record was given as a key: " + k);
        });

    // Resolve all reachable classes in the record hierarchy
    final var recordClassHierarchy = recordClassHierarchy(clazz);

    // As classes may be renamed or moved, we need to ensure that the type signatures map given by the user is a reachable class
    typeSignatures.keySet().stream().filter(k -> !recordClassHierarchy.contains(k))
        .findAny()
        .ifPresent(k -> {
          throw new IllegalArgumentException("Type signatures map must not contain a class that is not in the record " +
              "hierarchy for class " + clazz + " was given as a key: " + k + " which is not one of " +
              recordClassHierarchy.stream().map(Class::getName).collect(Collectors.joining(",")));
        });

    // Partition classes into legal and illegal
    final Map<Boolean, List<Class<?>>> legalAndIllegalClasses = recordClassHierarchy.stream()
        .collect(Collectors.partitioningBy(cls ->
            cls.isRecord() || cls.isEnum() || cls.isSealed() || cls.isArray() || cls.isPrimitive()
                || String.class.equals(cls) || UUID.class.isAssignableFrom(cls)
                || List.class.isAssignableFrom(cls) || Optional.class.isAssignableFrom(cls)
                || Map.class.isAssignableFrom(cls) || BOXED_PRIMITIVES.contains(cls)
        ));

    // Check for illegal classes
    final var illegalClasses = legalAndIllegalClasses.get(Boolean.FALSE);
    if (!illegalClasses.isEmpty()) {
      throw new IllegalArgumentException("Illegal types found: " +
          illegalClasses.stream().map(Class::getName).collect(Collectors.joining(", ")));
    }

    // Filter out record classes that we need to make RecordSerde for
    final var recordClasses = legalAndIllegalClasses.get(Boolean.TRUE).stream()
        .filter(Class::isRecord)
        .toList();
    if (recordClasses.isEmpty()) {
      throw new IllegalArgumentException("No record classes found in hierarchy of: " + clazz);
    }

    // Compute enum type signatures using streams
    @SuppressWarnings("unchecked") final Map<Class<Enum<?>>, Long> enumToTypeSignatureMap = legalAndIllegalClasses.get(Boolean.TRUE).stream()
        .filter(Class::isEnum)
        .map(cls -> (Class<Enum<?>>) cls)
        .filter(Enum.class::isAssignableFrom)
        .collect(Collectors.toMap(
            enumClass -> enumClass,
            Companion::hashEnumSignature
        ));

    LOGGER.fine(() -> "Creating PicklerImpl for records: " +
        recordClasses.stream().map(Class::getName).collect(Collectors.joining(",")) +
        " and enums: " + enumToTypeSignatureMap.keySet().stream().map(Class::getName).collect(Collectors.joining(","))
    );

    // FIXME if there is only one Serde pickler class we can just return it and save the map lookup as long as we handle the type signature correctly
    return new PicklerImpl<>(recordClasses, enumToTypeSignatureMap, typeSignatures);
  }

  /// In order to support optional backwards compatibility, we need to be able to tell the newer pickler what is the
  /// type signature of the original class.
  long typeSignature(Class<?> originalClass);
}
