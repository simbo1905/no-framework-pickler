// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//

package io.github.simbo1905.no.framework;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

import static io.github.simbo1905.no.framework.Companion.hashClassSignature;

/// Specialized handler for records with no components (empty records)
final class EmptyRecordSerde<T> implements Pickler<T> {
  final Class<?> userType;
  final long typeSignature;
  final T singleton;

  @SuppressWarnings("unchecked")
  public EmptyRecordSerde(@NotNull Class<?> userType) {
    assert userType.isRecord() : "User type must be a record: " + userType;

    final var components = userType.getRecordComponents();
    assert components != null && components.length == 0 : "Empty record must have no components: " + userType;

    this.userType = userType;
    this.typeSignature = hashClassSignature(userType, components, new TypeExpr[0]);

    // Create singleton instance
    try {
      final var constructor = userType.getDeclaredConstructor();

      this.singleton = (T) constructor.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create empty record instance for " + userType, e);
    }

    LOGGER.info(() -> "EmptyRecordSerde construction complete for " + userType.getSimpleName());
  }

  @Override
  public int serialize(ByteBuffer buffer, T record) {
    Objects.requireNonNull(buffer);
    Objects.requireNonNull(record);
    buffer.order(ByteOrder.BIG_ENDIAN);

    if (!userType.isAssignableFrom(record.getClass())) {
      throw new IllegalArgumentException("Expected " + userType + " but got " + record.getClass());
    }

    buffer.putLong(typeSignature);
    return Long.BYTES;
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

    return singleton;
  }

  @Override
  public int maxSizeOf(T record) {
    Objects.requireNonNull(record);
    if (!userType.isAssignableFrom(record.getClass())) {
      throw new IllegalArgumentException("Expected " + userType + " but got " + record.getClass());
    }

    return Long.BYTES; // Only type signature
  }

  @Override
  public String toString() {
    return "EmptyRecordSerde{userType=" + userType + ", typeSignature=0x" + Long.toHexString(typeSignature) + "}";
  }
}
