// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//

package io.github.simbo1905.no.framework;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.Optional;

/// Specialized handler for records with no components (empty records)
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
final class EmptyRecordSerde<T> implements Pickler<T> {
  final Class<?> userType;
  final long typeSignature;
  final Optional<Long> altTypeSignature;
  final T singleton;

  @SuppressWarnings("unchecked")
  public EmptyRecordSerde(@NotNull Class<?> userType, long typeSignature, Optional<Long> altTypeSignature) {
    assert userType.isRecord() : "User type must be a record: " + userType;

    final var components = userType.getRecordComponents();
    assert components != null && components.length == 0 : "Empty record must have no components: " + userType;

    this.userType = userType;
    this.typeSignature = typeSignature;
    this.altTypeSignature = altTypeSignature;

    // Create singleton instance
    try {
      final var constructor = userType.getDeclaredConstructor();

      this.singleton = (T) constructor.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create empty record instance for " + userType, e);
    }

    LOGGER.fine(() -> "EmptyRecordSerde " + userType.getName() + " construction complete with type signature 0x" +
        Long.toHexString(typeSignature));
  }

  @Override
  public int serialize(ByteBuffer buffer, T record) {
    Objects.requireNonNull(buffer);
    Objects.requireNonNull(record);
    buffer.order(ByteOrder.BIG_ENDIAN);

    if (!userType.isAssignableFrom(record.getClass())) {
      throw new IllegalArgumentException("Expected " + userType + " but got " + record.getClass());
    }
    LOGGER.fine(() -> "EmptyRecordSerde " + userType.getName() + " Serializing empty record " + userType.getSimpleName() + " with type signature 0x" +
        Long.toHexString(typeSignature) + " at position " + buffer.position());
    buffer.putLong(typeSignature);
    LOGGER.finer(() -> "EmptyRecordSerde wrote type signature 0x" + Long.toHexString(typeSignature) + " at position " + (buffer.position() - Long.BYTES) + " to " + (buffer.position() - 1));
    ZigZagEncoding.putInt(buffer, 0); // Empty record has zero components
    LOGGER.finer(() -> "EmptyRecordSerde wrote component count 0 at position " + (buffer.position() - ZigZagEncoding.sizeOf(0)) + " to " + (buffer.position() - 1));
    return Long.BYTES + ZigZagEncoding.sizeOf(0); // Type signature + zero component count
  }

  @Override
  public T deserialize(ByteBuffer buffer) {
    Objects.requireNonNull(buffer);
    buffer.order(ByteOrder.BIG_ENDIAN);

    final long incomingSignature = buffer.getLong();
    if (incomingSignature != typeSignature) {
      throw new IllegalStateException("Type signature mismatch: expected 0x" +
          Long.toHexString(typeSignature) + " but got 0x" + Long.toHexString(incomingSignature));
    }
    final int count = ZigZagEncoding.getInt(buffer);
    assert count == 0 : "Empty record should have zero components, but got " + count;
    return singleton;
  }

  /// Package-private deserialization method that assumes the type signature has already been read
  /// and validated by the caller (e.g., RefValueReader)
  T deserializeWithoutSignature(ByteBuffer buffer) {
    Objects.requireNonNull(buffer);
    buffer.order(ByteOrder.BIG_ENDIAN);
    LOGGER.fine(() -> "EmptyRecordSerde " + userType.getSimpleName() +
        " deserializeWithoutSignature() at position " + buffer.position());

    final int count = ZigZagEncoding.getInt(buffer);
    LOGGER.finer(() -> "EmptyRecordSerde read component count " + count + " from position " + (buffer.position() - ZigZagEncoding.sizeOf(count)) + " to " + (buffer.position() - 1));
    assert count == 0 : "Empty record should have zero components, but got " + count;
    LOGGER.finer(() -> "EmptyRecordSerde returning singleton instance of " + userType.getSimpleName());
    return singleton;
  }

  @Override
  public int maxSizeOf(T record) {
    Objects.requireNonNull(record);
    if (!userType.isAssignableFrom(record.getClass())) {
      throw new IllegalArgumentException("Expected " + userType + " but got " + record.getClass());
    }

    return Byte.BYTES + Long.BYTES; // Only type signature and zero component count
  }

  @Override
  public long typeSignature(Class<?> originalClass) {
    Objects.requireNonNull(originalClass);
    if (!userType.isAssignableFrom(originalClass)) {
      throw new IllegalArgumentException("Expected " + userType + " but got " + originalClass);
    }
    return typeSignature;
  }

  @Override
  public String toString() {
    return "EmptyRecordSerde{userType=" + userType + ", typeSignature=0x" + Long.toHexString(typeSignature) + "}";
  }
}
