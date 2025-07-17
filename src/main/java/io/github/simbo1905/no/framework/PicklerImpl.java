// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//

package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

import static io.github.simbo1905.no.framework.Companion.computeRecordTypeSignatures;
import static io.github.simbo1905.no.framework.Companion.writeToWireWitness;

/// Main coordinator for multiple record types using static analysis and callback delegation
@Deprecated
final class PicklerImpl<R> implements Pickler<R> {
  final List<Class<?>> userTypes;
  final Map<Long, Pickler<?>> typeSignatureToPicklerMap;
  final Map<Class<?>, Long> recordClassToTypeSignatureMap;
  final Map<Class<Enum<?>>, Long> enumToTypeSignatureMap;
  final Map<Class<?>, Long> altTypeSignatures;
  final Map<Long, Pickler<?>> altTypeSignatureToPicklerMap;
  final boolean compatibilityMode;

  /// This constructor initializes the pickler with a list of record classes, a map of enum type signatures,
  /// and a map of type signatures for backwards compatibility.
  /// @param recordClasses List of record classes to be serialized.
  /// @param enumToTypeSignatureMap Map of enum classes to their type signatures.
  /// @param altTypeSignatures Map of class signatures to type signatures for backwards compatibility.
  PicklerImpl(final List<Class<?>> recordClasses,
              final Map<Class<Enum<?>>, Long> enumToTypeSignatureMap,
              final Map<Class<?>, Long> altTypeSignatures) {
    this.compatibilityMode = CompatibilityMode.current() == CompatibilityMode.ENABLED;
    this.userTypes = List.copyOf(recordClasses);
    this.enumToTypeSignatureMap = Map.copyOf(enumToTypeSignatureMap);
    this.altTypeSignatures = Map.copyOf(altTypeSignatures);
    // Compute type signatures using stream operations
    this.recordClassToTypeSignatureMap = computeRecordTypeSignatures(recordClasses);

    // Create record serdes with pre-built handlers using streams
    final Map<Class<?>, Pickler<?>> serdes = recordClasses.stream()
        .collect(Collectors.toMap(
            clz -> clz,
            this::createRecordSerde
        ));

    // Build type signature to pickler map using streams
    this.typeSignatureToPicklerMap = serdes.entrySet().stream()
        .collect(Collectors.toMap(
            entry -> recordClassToTypeSignatureMap.get(entry.getKey()),
            Map.Entry::getValue
        ));

    this.altTypeSignatureToPicklerMap = serdes.entrySet().stream()
        .filter(c -> altTypeSignatures.containsKey(c.getKey()))
        .collect(Collectors.toMap(
            entry -> altTypeSignatures.get(entry.getKey()),
            Map.Entry::getValue
        ));

    LOGGER.fine(() -> "PicklerImpl construction complete for " +
        recordClasses.stream().map(Class::getName).collect(Collectors.joining(", ")) +
        " with compatibility mode " + compatibilityMode);
  }


  Pickler<?> createRecordSerde(Class<?> clz) {
    final var altTypeSignature = Optional.ofNullable(altTypeSignatures.getOrDefault(clz, null));
    final var components = clz.getRecordComponents();
    if (components.length == 0) {
      return new EmptyRecordSerde<>(clz);
    } else {
      return new RecordSerde<>(clz, this::resolveTypeSizer, this::resolveTypeWriter, this::resolveTypeReader, altTypeSignature);
    }
  }

  /// Resolve sizer for complex types (RECORD, INTERFACE, ENUM)
  private Sizer resolveTypeSizer(Class<?> targetClass) {
    return obj -> {
      if (obj == null) return Byte.BYTES;

      if (obj instanceof Enum<?> ignored) {
        return Byte.BYTES + Long.BYTES + Integer.BYTES; // marker + typeSignature + ordinal
      }

      /// FIXME i think this is junk all the picklers are in both typeSignatureToPicklerMap and serdes used by resolvePicker
      if (userTypes.contains(obj.getClass())) {
        // Self-recursion fast path
        final var serde = typeSignatureToPicklerMap.get(
            recordClassToTypeSignatureMap.get(obj.getClass()));
        if (serde instanceof RecordSerde<?> recordSerde) {
          return Byte.BYTES + maxSizeOfWitness(obj, recordSerde);
        } else if (serde instanceof EmptyRecordSerde<?>) {
          return Byte.BYTES + Long.BYTES; // marker + typeSignature
        }
      }
      throw new AssertionError("not found sizer for " + obj.getClass().getName());
    };
  }

  /// Resolve writer for complex types
  Writer resolveTypeWriter(Class<?> targetClass) {
    return (buffer, obj) -> {
      if (obj instanceof Enum<?> enumValue) {
        final var typeSignature = enumToTypeSignatureMap.get(enumValue.getClass());
        final var ordinal = enumValue.ordinal();
        LOGGER.fine(() -> "PicklerImpl writing enumValue typeSignature " + Long.toHexString(typeSignature) +
            " and ordinal " + ordinal + " at position " + buffer.position());
        buffer.putLong(typeSignature);
        ZigZagEncoding.putInt(buffer, ordinal);
        return;
      }

      if (userTypes.contains(obj.getClass())) {
        // Self-recursion fast path
        final var serde = typeSignatureToPicklerMap.get(
            recordClassToTypeSignatureMap.get(obj.getClass()));
        writeToWireWitness(serde, buffer, obj);
        return;
      }
      throw new AssertionError("not found pickler for " + obj.getClass().getName());
    };
  }

  /// Resolve reader for complex types by type signature
  private Reader resolveTypeReader(Long typeSignature) {
    return buffer -> {
      if (typeSignature == 0L) {
        return null; // null marker
      }

      // Check if it's one of our record types
      if (typeSignatureToPicklerMap.containsKey(typeSignature)) {
        final var pickler = typeSignatureToPicklerMap.get(typeSignature);
        return switch (pickler) {
          case EmptyRecordSerde<?> ers -> ers.singleton;
          case RecordSerde<?> rs -> rs.readFromWire(buffer);
          default -> throw new AssertionError("Unexpected pickler type: " + pickler.getClass());
        };
      }

      // Check if it's an enum
      final var enumClass = enumToTypeSignatureMap.entrySet().stream()
          .filter(entry -> entry.getValue().equals(typeSignature))
          .map(Map.Entry::getKey)
          .findFirst()
          .orElseThrow(() -> new IllegalStateException("Unknown type signature: 0x" + Long.toHexString(typeSignature)));

      final int ordinal = ZigZagEncoding.getInt(buffer);
      final Enum<?>[] constants = enumClass.getEnumConstants();
      return constants[ordinal];
    };
  }

  @Override
  public int serialize(ByteBuffer buffer, R record) {
    Objects.requireNonNull(buffer);
    Objects.requireNonNull(record);

    if (!record.getClass().isRecord()) {
      throw new IllegalArgumentException("Expected record but got: " + record.getClass());
    }

    if (!userTypes.contains(record.getClass())) {
      throw new IllegalArgumentException("Record type " + record.getClass().getSimpleName() +
          " not in registered types: " + userTypes.stream()
          .map(Class::getSimpleName).collect(Collectors.joining(", ")));
    }

    final var typeSignature = recordClassToTypeSignatureMap.get(record.getClass());
    final var pickler = typeSignatureToPicklerMap.get(typeSignature);

    return switch (pickler) {
      case EmptyRecordSerde<?> ers -> writeToWireWitness(ers, buffer, record);
      case RecordSerde<?> rs -> writeToWireWitness(rs, buffer, record);
      default -> throw new AssertionError("Unexpected pickler type: " + pickler.getClass());
    };
  }

  /// Deserialize a record from a ByteBuffer using the type signature map
  /// @param buffer The ByteBuffer to read from
  /// @return The deserialized record
  /// @throws IllegalStateException if the type signature is unknown or not found in the maps
  @SuppressWarnings("unchecked")
  @Override
  public R deserialize(ByteBuffer buffer) {
    Objects.requireNonNull(buffer);
    buffer.order(ByteOrder.BIG_ENDIAN);
    final long typeSignature = buffer.getLong();
    final Pickler<?> pickler;
    if (typeSignatureToPicklerMap.containsKey(typeSignature)) {
      pickler = typeSignatureToPicklerMap.get(typeSignature);
    } else if (compatibilityMode && altTypeSignatureToPicklerMap.containsKey(typeSignature)) {
      pickler = altTypeSignatureToPicklerMap.get(typeSignature);
    } else {
      throw new IllegalStateException("Unknown type signature: 0x" + Long.toHexString(typeSignature) + " as compatibility mode is " +
          (compatibilityMode ? "enabled" : "disabled") + ". Known signatures: " +
          typeSignatureToPicklerMap.keySet().stream()
              .map(Long::toHexString).collect(Collectors.joining(", ")) +
          (compatibilityMode ? ", alt signatures: " +
              altTypeSignatureToPicklerMap.keySet().stream()
                  .map(Long::toHexString).collect(Collectors.joining(", ")) : ""));
    }

    return (R) switch (pickler) {
      case EmptyRecordSerde<?> ers -> ers.singleton;
      case RecordSerde<?> rs -> rs.readFromWire(buffer);
      default -> throw new AssertionError("Unexpected pickler type: " + pickler.getClass());
    };
  }

  @Override
  public int maxSizeOf(R record) {
    Objects.requireNonNull(record);
    if (!record.getClass().isRecord()) {
      throw new IllegalArgumentException("Expected record but got: " + record.getClass());
    }

    final var typeSignature = recordClassToTypeSignatureMap.get(record.getClass());
    final var pickler = typeSignatureToPicklerMap.get(typeSignature);
    return maxSizeOfWitness(record, pickler);
  }

  /// Get the type signature for a given record class that can be used for backwards compatibility
  /// @param originalClass The original record class
  /// @return The type signature for the record class
  /// @throws IllegalArgumentException if the class is not in the user types
  @Override
  public long typeSignature(Class<?> originalClass) {
    // throw an InvalidArgumentException if the class is not in the user types
    Objects.requireNonNull(originalClass, "Original class must not be null");
    if (!userTypes.contains(originalClass)) {
      throw new IllegalArgumentException("Class " + originalClass.getSimpleName() +
          " not in registered types: " + recordClassToTypeSignatureMap.keySet().stream()
          .map(Class::getSimpleName).collect(Collectors.joining(", ")));
    }
    return recordClassToTypeSignatureMap.get(originalClass);
  }

  @SuppressWarnings("unchecked")
  private static <R> int maxSizeOfWitness(Object record, Pickler<R> pickler) {
    return pickler.maxSizeOf((R) record);
  }

  @Override
  public String toString() {
    return "PicklerImpl{userTypes=" + userTypes.stream().map(Class::getSimpleName).collect(Collectors.joining(", ")) + "}";
  }
}
