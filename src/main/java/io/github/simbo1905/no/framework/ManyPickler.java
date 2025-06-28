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

final class ManyPickler<R> implements Pickler<R> {

  static final Map<Class<?>, Pickler<?>> REGISTRY = new ConcurrentHashMap<>();

  final List<Class<?>> userTypes;
  final Map<Long, Pickler<?>> typeSignatureToPicklerMap;
  final Map<Class<?>, Long> recordClassToTypeSignatureMap;
  final Map<Class<?>, Pickler<?>> picklers;
  final Map<Class<Enum<?>>, Long> enumToTypeSignatureMap;

  public ManyPickler(final List<Class<?>> recordClasses, Map<Class<Enum<?>>, Long> enumToTypeSignatureMap) {
    this.userTypes = recordClasses;
    this.enumToTypeSignatureMap = enumToTypeSignatureMap;

    // TODO had to not cache here due to circular cache updates that can be fixed later
    LOGGER.fine(() -> "ManyPickler resolve componentPicker for " + recordClasses.stream().map(Class::getSimpleName)
        .collect(Collectors.joining(", ")));
    picklers = recordClasses.stream().collect(Collectors.toMap(
        clz -> clz,
        c -> resolvePickerNoCache(c, enumToTypeSignatureMap)
    ));

    this.typeSignatureToPicklerMap = new ConcurrentHashMap<>();
    picklers.values().forEach(pickler -> {
      final long signature = switch (pickler) {
        case RecordPickler<?> rp -> rp.typeSignature;
        case EmptyRecordPickler<?> erp -> erp.typeSignature;
        default -> throw new IllegalArgumentException("Unexpected pickler type: " + pickler.getClass());
      };
      LOGGER.finer(() -> "Registering type signature: 0x" + Long.toHexString(signature) + " for pickler: " + pickler.getClass().getSimpleName());
      typeSignatureToPicklerMap.put(signature, pickler);
    });
    LOGGER.fine(() -> "ManyPickler typeSignatureToPicklerMap contents: " +
        typeSignatureToPicklerMap.entrySet().stream()
            .map(entry -> "0x" + Long.toHexString(entry.getKey()) + " -> " + entry.getValue())
            .collect(Collectors.joining(", ")));

    this.recordClassToTypeSignatureMap = picklers.entrySet().stream().map(
            classAndPickler -> Map.entry(classAndPickler.getKey(),
                switch (classAndPickler.getValue()) {
                  case RecordPickler<?> rp -> rp.typeSignature;
                  case EmptyRecordPickler<?> erp -> erp.typeSignature;
                  default -> throw new IllegalArgumentException("Unexpected pickler type: " + classAndPickler.getValue());
                })
        )
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    LOGGER.fine(() -> "ManyPickler construction complete for user types: " +
        userTypes.stream().map(Class::getSimpleName).collect(Collectors.joining(", ")));
  }

  static @NotNull Pickler<?> componentPicker(Class<?> userType,
                                             Map<Class<Enum<?>>, Long> enumToTypeSignatureMap) {
    Objects.requireNonNull(userType, "User type must not be null");
    assert userType.isRecord() : "User type must be a record type: " + userType;
    RecordComponent[] components = userType.getRecordComponents();
    if (components.length == 0) {
      //noinspection rawtypes,unchecked
      return new EmptyRecordPickler(userType);
    } else {
      //noinspection rawtypes,unchecked
      return new RecordPickler(userType, enumToTypeSignatureMap);
    }
  }

  @Override
  public int serialize(ByteBuffer buffer, R record) {
    Objects.requireNonNull(buffer, "Buffer must not be null");
    Objects.requireNonNull(record, "Record must not be null");
    if (record.getClass().isRecord()) {
      Class<?> userType = record.getClass();
      if (!userTypes.contains(userType)) {
        throw new IllegalArgumentException("Record type " + userType.getSimpleName() +
            " is not one of the registered user types: " + userTypes.stream()
            .map(Class::getSimpleName).collect(Collectors.joining(", ")));
      }
      final var pickler = this.picklers.get(userType);
      LOGGER.fine(() -> "ManyPickler Serializing record  " + userType.getSimpleName() +
          " hashCode " + record.hashCode() +
          " position " + buffer.position() +
          " buffer remaining bytes: " + buffer.remaining() + " limit: " +
          buffer.limit() + " capacity: " + buffer.capacity()
      );
      // FIXME we should also handle EmptyRecordPickler here
      @SuppressWarnings("unchecked") final var r = ((RecordPickler<R>) pickler).writeToWire(buffer, record);
      return r;
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
    if (!typeSignatureToPicklerMap.containsKey(typeSignature)) {
      throw new IllegalArgumentException("ManyPickler no pickler found for typeSignature: " + typeSignature + " at position " + startPosition +
          " buffer remaining bytes: " + buffer.remaining() + " limit: " +
          buffer.limit() + " capacity: " + buffer.capacity());
    }
    final var pickler = typeSignatureToPicklerMap.get(typeSignature);
    LOGGER.fine(() -> "ManyPickler deserializing position " + startPosition +
        " typeSignature 0x" + Long.toHexString(typeSignature) +
        " buffer remaining bytes: " + buffer.remaining() + " limit: " +
        buffer.limit() + " capacity: " + buffer.capacity()
    );
    switch (pickler) {
      case RecordPickler<?> rp -> {
        // The type signature was already validated at the root level
        LOGGER.fine(() -> "RecordPickler deserializing record of type " + rp.userType.getSimpleName() +
            " at position: " + buffer.position() +
            " with type signature: 0x" + Long.toHexString(typeSignature));
        //noinspection unchecked
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
    final var pickler = resolvePicker(userType, enumToTypeSignatureMap);
    return pickler.maxSizeOf(record);
  }

  static <R> @NotNull Pickler<R> resolvePicker(Class<?> userType,
                                               Map<Class<Enum<?>>, Long> enumToTypeSignatureMap) {
    LOGGER.fine(() -> "ManyPickler " + userType + " resolve componentPicker for userType: " + userType.getSimpleName());
    final var pickler = REGISTRY.computeIfAbsent(userType, aClass -> componentPicker(userType, enumToTypeSignatureMap));
    //noinspection unchecked
    return (Pickler<R>) pickler;
  }

  static <R> @NotNull Pickler<R> resolvePickerNoCache(Class<?> userType,
                                                      Map<Class<Enum<?>>, Long> enumToTypeSignatureMap) {
    //noinspection unchecked
    return (Pickler<R>) componentPicker(userType, enumToTypeSignatureMap);
  }

  @Override
  public String toString() {
    return "ManyPickler{" +
        "typeSignatureToPicklerMap=" + typeSignatureToPicklerMap.entrySet().stream()
        .map(e -> e.getKey() + "->" + e.getValue()) +
        '}';
  }
}
