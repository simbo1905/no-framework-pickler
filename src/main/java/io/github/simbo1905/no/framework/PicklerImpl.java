// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//

package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

import static io.github.simbo1905.no.framework.Companion.computeRecordTypeSignatures;
import static io.github.simbo1905.no.framework.Companion.writeToWireWitness;

/// Main coordinator for multiple record types using static analysis and callback delegation
final class PicklerImpl<R> implements Pickler<R> {
  final List<Class<?>> userTypes;
  /// FIXME I think this is duplicated between typeSignatureToPicklerMap and serdes
  final Map<Long, Pickler<?>> typeSignatureToPicklerMap;
  final Map<Class<?>, Pickler<?>> serdes;
  final Map<Class<?>, Long> recordClassToTypeSignatureMap;
  final Map<Class<Enum<?>>, Long> enumToTypeSignatureMap;


  public PicklerImpl(List<Class<?>> recordClasses, Map<Class<Enum<?>>, Long> enumToTypeSignatureMap) {
    Objects.requireNonNull(recordClasses);
    Objects.requireNonNull(enumToTypeSignatureMap);

    this.userTypes = List.copyOf(recordClasses);
    this.enumToTypeSignatureMap = Map.copyOf(enumToTypeSignatureMap);

    // Compute type signatures using stream operations
    this.recordClassToTypeSignatureMap = computeRecordTypeSignatures(recordClasses);

    // Create record serdes with pre-built handlers using streams
    serdes = recordClasses.stream()
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

    LOGGER.fine(() -> "PicklerImpl construction complete for " +
        recordClasses.stream().map(Class::getSimpleName).collect(Collectors.joining(", ")));
  }

  Pickler<?> createRecordSerde(Class<?> clz) {
    final var components = clz.getRecordComponents();
    if (components.length == 0) {
      return new EmptyRecordSerde<>(clz);
    } else {
      return new RecordSerde<>(clz, this::resolveTypeSizer, this::resolveTypeWriter, this::resolveTypeReader);
    }
  }

  /// Resolve sizer for complex types (RECORD, INTERFACE, ENUM)
  private ToIntFunction<Object> resolveTypeSizer(Class<?> targetClass) {
    return obj -> {
      if (obj == null) return Byte.BYTES;

      if (obj instanceof Enum<?> ignored) {
        return Byte.BYTES + Long.BYTES + Integer.BYTES; // marker + typeSignature + ordinal
      }

      /// FIXME i think this is junk all the picklers are in both typeSignatureToPicklerMap and serdes used by resolvePicker
      if (userTypes.contains(obj.getClass())) {
        // Self-recursion fast path
        @SuppressWarnings("unchecked") final var serde = (RecordSerde<Object>) typeSignatureToPicklerMap.get(
            recordClassToTypeSignatureMap.get(obj.getClass()));
        return Byte.BYTES + serde.maxSizeOf(obj);
      }

      // Delegate to other pickler
      @SuppressWarnings("unchecked") final RecordSerde<Object> otherPickler = (RecordSerde<Object>) resolvePicker(obj.getClass());
      return Byte.BYTES + otherPickler.maxSizeOf(obj);
    };
  }

  /// Resolve writer for complex types
  BiConsumer<ByteBuffer, Object> resolveTypeWriter(Class<?> targetClass) {
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
        final var serde = (RecordSerde<?>) typeSignatureToPicklerMap.get(
            recordClassToTypeSignatureMap.get(obj.getClass()));
        writeToWireWitness(serde, buffer, obj);
        return;
      }

      // Delegate to other pickler
      final var otherPickler = resolvePicker(obj.getClass());
      writeToWireWitness(otherPickler, buffer, obj);
    };
  }

  /// FIXME I think this is duplicated between typeSignatureToPicklerMap and serdes
  @SuppressWarnings("unchecked")
  <X> Pickler<X> resolvePicker(Class<X> aClass) {
    return (Pickler<X>) serdes.get(aClass);
  }

  /// Resolve reader for complex types by type signature
  private Function<ByteBuffer, Object> resolveTypeReader(Long typeSignature) {
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

//  private static Pickler<?> resolvePicker(Class<?> clz) {
//    // Use existing ManyPickler.resolvePicker for now - this could be improved
//    return PicklerImpl.resolvePicker(clz, enumToTypeSignatureMap);
//  }

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
      case EmptyRecordSerde<?> ers -> {
        buffer.putLong(ers.typeSignature);
        yield Long.BYTES;
      }
      case RecordSerde<?> rs -> writeToWireWitness(rs, buffer, record);
      default -> throw new AssertionError("Unexpected pickler type: " + pickler.getClass());
    };
  }

  @SuppressWarnings("unchecked")
  @Override
  public R deserialize(ByteBuffer buffer) {
    Objects.requireNonNull(buffer);
    buffer.order(ByteOrder.BIG_ENDIAN);

    final long typeSignature = buffer.getLong();
    if (!typeSignatureToPicklerMap.containsKey(typeSignature)) {
      throw new IllegalArgumentException("Unknown type signature: 0x" + Long.toHexString(typeSignature));
    }

    final var pickler = typeSignatureToPicklerMap.get(typeSignature);
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

  @SuppressWarnings("unchecked")
  private static <R> int maxSizeOfWitness(Object record, Pickler<R> pickler) {
    return pickler.maxSizeOf((R) record);
  }

  @Override
  public String toString() {
    return "PicklerImpl{userTypes=" + userTypes.stream().map(Class::getSimpleName).collect(Collectors.joining(", ")) + "}";
  }
}
