// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

import static io.github.simbo1905.no.framework.RecordPickler.SHA_256;

public final class EmptyRecordPickler<T> implements Pickler<T> {

  final Class<?> userType;

  final Object singleton;

  final long typeSignature;

  public EmptyRecordPickler(Class<?> userType) {
    Objects.requireNonNull(userType);
    assert userType.isRecord();
    this.userType = userType;
    var components = userType.getRecordComponents();
    if (components != null && components.length != 0) {
      throw new IllegalArgumentException("EmptyRecordPickler requires zero components " +
          "but  " + userType.getName() + " has components: " + components.length);
    }
    // use reflection to create a singleton instance as it has a // zero-argument constructor
    try {
      this.singleton = userType.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create singleton instance for " + userType.getName(), e);
    }
    long result;
    try {
      final String uniqueNess = userType.getSimpleName();
      result = RecordPickler.hashSignature(uniqueNess);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(SHA_256 + " not available", e);
    }
    this.typeSignature = result;
    LOGGER.fine(() -> "EmptyRecordPickler construction complete for " + userType.getSimpleName());
  }

  @Override
  public int serialize(ByteBuffer buffer, T record) {
    Objects.requireNonNull(buffer, "buffer cannot be null");
    Objects.requireNonNull(record, "record cannot be null");
    buffer.order(ByteOrder.BIG_ENDIAN);
    final var clz = record.getClass();
    if (this.userType.isAssignableFrom(clz)) {
      final int startPosition = buffer.position();
      LOGGER.fine(() -> "EmptyRecordPickler serialize record " + record.getClass().getSimpleName() +
          " hashCode " + record.hashCode() +
          " position " + startPosition +
          " typeSignature 0x" + Long.toHexString(typeSignature) +
          " buffer remaining bytes: " + buffer.remaining() + " limit: " +
          buffer.limit() + " capacity: " + buffer.capacity()
      );
      buffer.putLong(this.typeSignature);
      return Long.BYTES;
    } else {
      throw new IllegalArgumentException("EmptyRecordPickler cannot serialize " +
          clz.getName() + " as it is not an instance of " + this.userType.getName());
    }
  }

  @Override
  public T deserialize(ByteBuffer buffer) {
    Objects.requireNonNull(buffer, "buffer cannot be null");
    buffer.order(ByteOrder.BIG_ENDIAN);
    final int typeSigPosition = buffer.position();
    // read the type signature first as it is a cryptographic hash of the class name and component metadata and fixed size
    final long signature = buffer.getLong();
    if (signature != this.typeSignature) {
      throw new IllegalStateException("EmptyRecordPickler expected type signature 0x" + Long.toHexString(this.typeSignature) +
          " but got: 0x" + Long.toHexString(signature) + " at position: " + typeSigPosition);
    }
    LOGGER.finer(() -> "EmptyRecordPickler deserialize " + this.userType.getSimpleName() + " position " +
        typeSigPosition + " signature 0x" + Long.toHexString(signature) +
        " buffer remaining bytes: " + buffer.remaining() + " limit: " +
        buffer.limit() + " capacity: " + buffer.capacity());

    @SuppressWarnings("unchecked") final var result = (T) this.singleton; // return the singleton instance
    return result;
  }

  @Override
  public int maxSizeOf(T record) {
    return Long.BYTES; // only the type signature is serialized
  }

  @Override
  public String toString() {
    return "EmptyRecordPickler{" +
        "userType=" + userType +
        ", typeSignature=" + typeSignature +
        '}';
  }
}
