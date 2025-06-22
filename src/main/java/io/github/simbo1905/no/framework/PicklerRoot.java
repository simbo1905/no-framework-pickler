// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

final class PicklerRoot<T> implements Pickler<T> {

  @SuppressWarnings("rawtypes")
  public static final Map<Class, RecordPickler> REGISTRY = new ConcurrentHashMap<>();

  final Class<?>[] userTypes;
  final Pickler<?>[] picklers;
  final Map<Long, RecordPickler<?>> typeSignatureToPicklerMap;
  final Map<Class<?>, Long> recordClassToTypeSignatureMap;

  public PicklerRoot(Class<?>[] sortedUserTypes) {
    this.userTypes = sortedUserTypes;
    this.picklers = new Pickler<?>[sortedUserTypes.length];
    final Map<Long, RecordPickler<?>> typeSignatureToPickler = new HashMap<>();
    final Map<Class<?>, Long> recordClassToTypeSignature = new HashMap<>();
    // To avoid null picklers in the array, we use NilPickler for non-record types
    Arrays.setAll(this.picklers, i -> {
      final var clazz = sortedUserTypes[i];
      if (clazz == null) {
        throw new IllegalArgumentException("User type cannot be null at index " + i);
      }
      if (!clazz.isRecord()) {
        return NilPickler.INSTANCE;
      } else {
        // Only create a given RecordPickler once per class and cache it.
        //noinspection rawtypes,unchecked
        final var p = REGISTRY.computeIfAbsent(clazz, aClass -> new RecordPickler(aClass, sortedUserTypes));
        final long typeSignature = p.typeSignature;
        typeSignatureToPickler.put(typeSignature, (RecordPickler<?>) p);
        recordClassToTypeSignature.put(clazz, typeSignature);
        return p;
      }
    });
    this.typeSignatureToPicklerMap = Map.copyOf(typeSignatureToPickler);
    this.recordClassToTypeSignatureMap = Map.copyOf(recordClassToTypeSignature);
    LOGGER.info(() -> "PicklerRoot construction complete - ready for high-performance serialization");
  }

  @Override
  public int serialize(ByteBuffer buffer, T record) {
    Objects.requireNonNull(buffer, "Buffer must not be null");
    Objects.requireNonNull(record, "Record must not be null");
    if (!record.getClass().isRecord()) {
      throw new IllegalArgumentException("Record must be a record type: " + record.getClass());
    }
    final var pickler = REGISTRY.getOrDefault(record.getClass(), null);
    if (pickler == null) {
      throw new IllegalArgumentException("No pickler registered for record type: " + record.getClass());
    }
    // Write out the ordinal of the record type to the buffer
    final Long typeSignature = recordClassToTypeSignatureMap.get(record.getClass());
    if (typeSignature == null) {
      throw new IllegalArgumentException("No type signature found for record type: " + record.getClass());
    }
    LOGGER.fine(() -> "Serializing record of type " + record.getClass() + " with ordinal " + typeSignature);
    buffer.putLong(typeSignature);
    //noinspection unchecked
    return pickler.serialize(buffer, record);
  }

  @Override
  public T deserialize(ByteBuffer buffer) {
    Objects.requireNonNull(buffer, "Buffer must not be null");
    // Read the ordinal of the record type from the buffer
    final int ordinal = ZigZagEncoding.getInt(buffer);
    if (ordinal < 0 || ordinal >= picklers.length) {
      throw new IllegalArgumentException("Invalid ordinal: " + ordinal + ". Must be between 0 and " + (picklers.length - 1));
    }
    final var pickler = typeSignatureToPicklerMap.get(ordinal);
    if (pickler == null) {
      throw new IllegalArgumentException("No pickler found for ordinal: " + ordinal);
    }
    //noinspection unchecked
    return (T) pickler.deserialize(buffer);
  }

  @Override
  public int maxSizeOf(T record) {
    Objects.requireNonNull(record);
    if (!record.getClass().isRecord()) {
      throw new IllegalArgumentException("Record must be a record type: " + record.getClass());
    }
    final var pickler = REGISTRY.getOrDefault(record.getClass(), null);
    if (pickler == null) {
      throw new IllegalArgumentException("No pickler registered for record type: " + record.getClass());
    }
    //noinspection unchecked
    return pickler.maxSizeOf(record);
  }
}
