// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.jetbrains.annotations.NotNull;

import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

final class PicklerRoot<R> implements Pickler<R> {

  static final Map<Class<?>, Pickler<?>> REGISTRY = new ConcurrentHashMap<>();

  final List<Class<?>> userTypes;
  final Map<Long, Pickler<?>> typeSignatureToPicklerMap;
  final Map<Class<?>, Long> recordClassToTypeSignatureMap;
  final Map<Class<?>, Pickler<?>> picklers;

  public PicklerRoot(final List<Class<?>> sortedUserTypes) {
    this.userTypes = sortedUserTypes;
    // To avoid null picklers in the array, we use NilPickler for non-record types
    picklers = sortedUserTypes.stream().collect(Collectors.toMap(
        clz -> clz,
        clz -> REGISTRY.computeIfAbsent(clz, aClass -> componentPicker(clz))
    ));

    this.typeSignatureToPicklerMap = picklers.values().stream().map(
            pickler -> Map.entry(
                switch (pickler) {
                  case RecordPickler<?> rp -> rp.typeSignature;
                  case EmptyRecordPickler<?> erp -> erp.typeSignature;
                  default -> throw new IllegalArgumentException("Unexpected pickler type: " + pickler.getClass());
                }, pickler)
        )
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    this.recordClassToTypeSignatureMap = picklers.entrySet().stream().map(
            classAndPickler -> Map.entry(classAndPickler.getKey(),
                switch (classAndPickler.getValue()) {
                  case RecordPickler<?> rp -> rp.typeSignature;
                  case EmptyRecordPickler<?> erp -> erp.typeSignature;
                  default -> throw new IllegalArgumentException("Unexpected pickler type: " + classAndPickler.getValue());
                })
        )
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    LOGGER.fine(() -> "PicklerRoot construction complete for user types: " +
        userTypes.stream().map(Class::getSimpleName).collect(Collectors.joining(", ")));
  }

  static @NotNull Pickler<?> componentPicker(Class<?> userType) {
    RecordComponent[] components = userType.getRecordComponents();
    if (components.length == 0) {
      //noinspection rawtypes,unchecked
      return new EmptyRecordPickler(userType);
    } else {
      //noinspection rawtypes,unchecked
      return new RecordPickler(userType);
    }
  }

  @Override
  public int serialize(ByteBuffer buffer, R record) {
    Objects.requireNonNull(buffer, "Buffer must not be null");
    Objects.requireNonNull(record, "Record must not be null");
    if (record.getClass().isRecord()) {
      Class<?> userType = record.getClass();
      final var pickler = resolvePicker(userType);
      // Write out the ordinal of the record type to the buffer
      final Long typeSignature = recordClassToTypeSignatureMap.get(record.getClass());
      if (typeSignature == null) {
        throw new IllegalArgumentException("No type signature found for record type: " + record.getClass());
      }

      LOGGER.fine(() -> "PicklerRoot Serializing record  " + userType.getSimpleName() +
          " hashCode " + record.hashCode() +
          " position " + buffer.position() +
          " typeSignature 0x" + Long.toHexString(typeSignature) +
          " buffer remaining bytes: " + buffer.remaining() + " limit: " +
          buffer.limit() + " capacity: " + buffer.capacity()
      );
      //noinspection,unchecked
      return ((RecordPickler<R>) pickler).writeToWire(buffer, record);
    } else {
      throw new IllegalArgumentException("Record must be a record type: " + record.getClass());
    }
  }

  @Override
  public R deserialize(ByteBuffer buffer) {
    Objects.requireNonNull(buffer, "Buffer must not be null");
    buffer.order(ByteOrder.BIG_ENDIAN);
    final long startPosition = buffer.position();
    final var typeSignature = buffer.getLong();
    final var pickler = typeSignatureToPicklerMap.get(typeSignature);

    if (pickler == null) {
      throw new IllegalArgumentException("PicklerRoot no pickler found for typeSignature: " + typeSignature + " at position " + startPosition +
          " buffer remaining bytes: " + buffer.remaining() + " limit: " +
          buffer.limit() + " capacity: " + buffer.capacity());
    }

    LOGGER.fine(() -> "PicklerRoot deserializing position " + startPosition +
        " typeSignature 0x" + Long.toHexString(typeSignature) +
        " buffer remaining bytes: " + buffer.remaining() + " limit: " +
        buffer.limit() + " capacity: " + buffer.capacity()
    );

    switch (pickler) {
      case RecordPickler<?> rp -> {
        // The type signature was already validated at the root level
        return (R) rp.readFromWire(buffer);
      }
      case EmptyRecordPickler<?> erp -> {
        // EmptyRecordPickler is a special case for records with no components
        //noinspection unchecked
        return (R) erp.singleton;
      }
      default -> throw new IllegalArgumentException("Unexpected pickler type: " + pickler.getClass());
    }
  }

  @Override
  public int maxSizeOf(R record) {
    Objects.requireNonNull(record);
    final Class<?> userType = record.getClass();
    if (!userType.isRecord()) {
      throw new IllegalArgumentException("Record must be a record type: " + record.getClass());
    }
    final var pickler = resolvePicker(userType);
    return pickler.maxSizeOf(record);
  }

  static <R> @NotNull Pickler<R> resolvePicker(Class<?> userType) {
    LOGGER.fine(() -> "PicklerRoot resolvePicker for userType: " + userType.getSimpleName());
    final var pickler = REGISTRY.computeIfAbsent(userType, aClass -> componentPicker(userType));
    //noinspection unchecked
    return (Pickler<R>) pickler;
  }
}
