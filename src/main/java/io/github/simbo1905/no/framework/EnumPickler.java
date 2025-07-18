// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//

package io.github.simbo1905.no.framework;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.Optional;

/// Specialized handler for enum types
final class EnumPickler<T extends Enum<T>> implements Pickler<T> {
  final Class<T> enumType;
  final long typeSignature;
  final Optional<Long> altTypeSignature;
  final T[] enumConstants;

  @SuppressWarnings("unchecked")
  public EnumPickler(@NotNull Class<T> enumType, long typeSignature, Optional<Long> altTypeSignature) {
    assert enumType.isEnum() : "User type must be an enum: " + enumType;

    this.enumType = enumType;
    this.typeSignature = typeSignature;
    this.altTypeSignature = altTypeSignature;
    this.enumConstants = enumType.getEnumConstants();

    LOGGER.fine(() -> "EnumPickler " + enumType.getName() + " construction complete with type signature 0x" +
        Long.toHexString(typeSignature));
  }

  @Override
  public int serialize(ByteBuffer buffer, T enumValue) {
    Objects.requireNonNull(buffer);
    Objects.requireNonNull(enumValue);
    buffer.order(ByteOrder.BIG_ENDIAN);

    if (!enumType.isAssignableFrom(enumValue.getClass())) {
      throw new IllegalArgumentException("Expected " + enumType + " but got " + enumValue.getClass());
    }
    LOGGER.fine(() -> "EnumPickler " + enumType.getName() + " Serializing enum " + enumValue + " with type signature 0x" +
        Long.toHexString(typeSignature) + " at position " + buffer.position());
    buffer.putLong(typeSignature);
    LOGGER.finer(() -> "EnumPickler wrote type signature 0x" + Long.toHexString(typeSignature) + " at position " + (buffer.position() - Long.BYTES) + " to " + (buffer.position() - 1));
    ZigZagEncoding.putInt(buffer, enumValue.ordinal());
    LOGGER.finer(() -> "EnumPickler wrote ordinal " + enumValue.ordinal() + " at position " + (buffer.position() - ZigZagEncoding.sizeOf(enumValue.ordinal())) + " to " + (buffer.position() - 1));
    return Long.BYTES + ZigZagEncoding.sizeOf(enumValue.ordinal());
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
    final int ordinal = ZigZagEncoding.getInt(buffer);
    if (ordinal < 0 || ordinal >= enumConstants.length) {
      throw new IllegalArgumentException("Invalid enum ordinal " + ordinal + " for " + enumType + " with " + enumConstants.length + " constants");
    }
    return enumConstants[ordinal];
  }

  /// Package-private deserialization method that assumes the type signature has already been read
  /// and validated by the caller (e.g., RefValueReader)
  T deserializeWithoutSignature(ByteBuffer buffer) {
    Objects.requireNonNull(buffer);
    buffer.order(ByteOrder.BIG_ENDIAN);
    LOGGER.fine(() -> "EnumPickler " + enumType.getSimpleName() +
        " deserializeWithoutSignature() at position " + buffer.position());

    final int ordinal = ZigZagEncoding.getInt(buffer);
    LOGGER.finer(() -> "EnumPickler read ordinal " + ordinal + " from position " + (buffer.position() - ZigZagEncoding.sizeOf(ordinal)) + " to " + (buffer.position() - 1));
    if (ordinal < 0 || ordinal >= enumConstants.length) {
      throw new IllegalArgumentException("Invalid enum ordinal " + ordinal + " for " + enumType + " with " + enumConstants.length + " constants");
    }
    LOGGER.finer(() -> "EnumPickler returning enum constant " + enumConstants[ordinal]);
    return enumConstants[ordinal];
  }

  @Override
  public int maxSizeOf(T enumValue) {
    Objects.requireNonNull(enumValue);
    if (!enumType.isAssignableFrom(enumValue.getClass())) {
      throw new IllegalArgumentException("Expected " + enumType + " but got " + enumValue.getClass());
    }

    return Long.BYTES + Integer.BYTES; // Type signature + ordinal
  }

  @Override
  public long typeSignature(Class<?> originalClass) {
    Objects.requireNonNull(originalClass);
    if (!enumType.isAssignableFrom(originalClass)) {
      throw new IllegalArgumentException("Expected " + enumType + " but got " + originalClass);
    }
    return typeSignature;
  }

  @Override
  public String toString() {
    return "EnumPickler{enumType=" + enumType + ", typeSignature=0x" + Long.toHexString(typeSignature) + "}";
  }
}