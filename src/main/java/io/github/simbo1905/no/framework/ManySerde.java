// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//

package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/// Main coordinator for multiple record types using static analysis and callback delegation
record ManySerde<R>(Class<R> rootClass, Map<Class<?>, Pickler<?>> serdes,
                    Map<Long, Pickler<?>> typeSignatureToSerde) implements Pickler<R> {
  ManySerde(Class<R> rootClass,
            Map<Class<?>, Pickler<?>> serdes,
            Map<Long, Pickler<?>> typeSignatureToSerde) {
    this.rootClass = rootClass;
    this.serdes = Map.copyOf(serdes);
    this.typeSignatureToSerde = Map.copyOf(typeSignatureToSerde);
  }


  @Override
  public int serialize(ByteBuffer buffer, R record) {
    Objects.requireNonNull(buffer);
    Objects.requireNonNull(record);
    @SuppressWarnings("unchecked") final var serde = (Pickler<R>) serdes.get(record.getClass());
    if (serde == null) {
      throw new IllegalArgumentException("No serde found for class: " + record.getClass());
    }
    return serde.serialize(buffer, record);
  }

  @Override
  public R deserialize(ByteBuffer buffer) {
    Objects.requireNonNull(buffer);
    final long typeSignature = buffer.getLong();
    LOGGER.fine(() -> "ManySerde.deserialize() looking up type signature: 0x" + Long.toHexString(typeSignature));
    final var serde = typeSignatureToSerde.get(typeSignature);
    if (serde == null) {
      LOGGER.severe(() -> "No serde found for type signature: 0x" + Long.toHexString(typeSignature) + ". Available signatures: " +
          typeSignatureToSerde.keySet().stream().map(sig -> "0x" + Long.toHexString(sig)).collect(Collectors.joining(", ")));
      throw new IllegalStateException("Unknown type signature: " + typeSignature);
    }
    LOGGER.fine(() -> "ManySerde.deserialize() found serde: " + serde.getClass().getSimpleName() + " for signature: 0x" + Long.toHexString(typeSignature));
    final R result;
    switch (serde) {
      case RecordSerde<?> recordSerde -> {
        @SuppressWarnings("unchecked")
        R record = (R) recordSerde.deserializeWithoutSignature(buffer);
        result = record;
      }
      case EmptyRecordSerde<?> emptyRecordSerde -> {
        @SuppressWarnings("unchecked")
        R record = (R) emptyRecordSerde.deserializeWithoutSignature(buffer);
        result = record;
      }
      case EnumSerde<?> enumPickler -> {
        @SuppressWarnings("unchecked")
        R record = (R) enumPickler.deserializeWithoutSignature(buffer);
        result = record;
      }
      default -> throw new IllegalStateException("Unsupported serde type: " + serde.getClass());
    }
    return result;
  }

  @Override
  public int maxSizeOf(R record) {
    Objects.requireNonNull(record);
    @SuppressWarnings("unchecked") final var serde = (Pickler<R>) serdes.get(record.getClass());
    if (serde == null) {
      throw new IllegalArgumentException("No serde found for class: " + record.getClass());
    }
    return serde.maxSizeOf(record);
  }

  @Override
  public long typeSignature(Class<?> originalClass) {
    final var serde = serdes.get(originalClass);
    if (serde == null) {
      throw new IllegalArgumentException("No serde found for class: " + originalClass);
    }
    return serde.typeSignature(originalClass);
  }
}
