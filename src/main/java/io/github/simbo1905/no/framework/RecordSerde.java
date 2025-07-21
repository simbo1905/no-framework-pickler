// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

/// Simplified record serialization handler focused on method handles and delegation
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
final class RecordSerde<T> implements Pickler<T> {
  final Class<?> userType;
  final long typeSignature; // Type signature is a hash of the class name and component metadata
  final Optional<Long> altTypeSignature; // Optional alternative type signature for backwards compatibility
  final MethodHandle recordConstructor;
  final MethodHandle[] componentAccessors;
  final Class<?>[] componentTypes; // Component types for the record, used for backwards compatibility defaults
  final boolean compatibilityMode;
  final Serde.Sizer[] sizers;
  final Serde.Writer[] writers;
  final Serde.Reader[] readers;

  RecordSerde(Class<?> userType,
              long typeSignature,
              Optional<Long> altTypeSignature,
              Serde.Sizer[] sizers, Serde.Writer[] writers, Serde.Reader[] readers) {
    assert userType.isRecord() : "User type must be a record: " + userType;
    compatibilityMode = CompatibilityMode.current() == CompatibilityMode.ENABLED;
    this.userType = Objects.requireNonNull(userType);
    this.typeSignature = typeSignature;
    this.altTypeSignature = Objects.requireNonNull(altTypeSignature);
    this.sizers = Objects.requireNonNull(sizers);
    this.writers = Objects.requireNonNull(writers);
    this.readers = Objects.requireNonNull(readers);

    final RecordComponent[] components = userType.getRecordComponents();
    assert components != null && components.length > 0 : "Record must have components: " + userType;
    assert sizers.length == writers.length && writers.length == readers.length :
        "Sizers, writers, and readers arrays must have the same length";

    // Create method handles
    try {
      final Class<?>[] parameterTypes = Arrays.stream(components)
          .map(RecordComponent::getType)
          .toArray(Class<?>[]::new);
      final Constructor<?> constructor = userType.getDeclaredConstructor(parameterTypes);
      this.recordConstructor = MethodHandles.lookup().unreflectConstructor(constructor);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create constructor handle for " + userType, e);
    }

    componentAccessors = Arrays.stream(components)
        .map(component -> {
          try {
            return MethodHandles.lookup().unreflect(component.getAccessor());
          } catch (Exception e) {
            throw new RuntimeException("Failed to create accessor for " + component.getName(), e);
          }
        })
        .toArray(MethodHandle[]::new);

    @SuppressWarnings("rawtypes") final var raw = (Class<?>[]) new Class[componentAccessors.length];
    componentTypes = raw;
    IntStream.range(0, componentAccessors.length).forEach(i -> {
      final RecordComponent rc = components[i];
      componentTypes[i] = rc.getType();
    });
  }

  @Override
  public int serialize(ByteBuffer buffer, T record) {
    Objects.requireNonNull(buffer);
    Objects.requireNonNull(record);
    buffer.order(ByteOrder.BIG_ENDIAN);
    if (!userType.isAssignableFrom(record.getClass())) {
      throw new IllegalArgumentException("Expected " + userType + " but got " + record.getClass());
    }
    return writeToWire(buffer, record);
  }

  int writeToWire(ByteBuffer buffer, T record) {
    final int startPosition = buffer.position();
    buffer.putLong(typeSignature);
    final int afterSigPosition = buffer.position();
    ZigZagEncoding.putInt(buffer, writers.length);
    final int afterCountPosition = buffer.position();

    IntStream.range(0, writers.length)
        .forEach(i -> writers[i].accept(buffer, record));

    final int endPosition = buffer.position();
    LOGGER.fine(() -> String.format("[%s.writeToWire] Wrote %d bytes for %s. Sig: 0x%s @%d, Count: %d @%d, Components @%d, End @%d",
        userType.getSimpleName(), (endPosition - startPosition), record.getClass().getSimpleName(),
        Long.toHexString(typeSignature), startPosition, writers.length, afterSigPosition, afterCountPosition, endPosition));
    return endPosition - startPosition;
  }

  /// Deserialize a record from the given buffer.
  /// When [CompatibilityMode] is DISABLED
  /// @throws IllegalStateException If the type signature does not match the expected value.
  @Override
  public T deserialize(ByteBuffer buffer) {
    Objects.requireNonNull(buffer);
    buffer.order(ByteOrder.BIG_ENDIAN);
    final int typeSigPosition = buffer.position();
    final long incomingSignature = buffer.getLong();
    LOGGER.fine(() -> String.format("[%s.deserialize] Read signature 0x%s @%d, expected 0x%s%s",
        userType.getSimpleName(), Long.toHexString(incomingSignature), typeSigPosition, Long.toHexString(typeSignature),
        altTypeSignature.map(alt -> " or 0x" + Long.toHexString(alt)).orElse("")));

    if (incomingSignature != this.typeSignature) {
      if (compatibilityMode && altTypeSignature.isPresent() && incomingSignature == altTypeSignature.get()) {
        LOGGER.info(() -> "Type signature mismatch, but compatibility mode is ENABLED. Proceeding with deserialization.");
      } else {
        throw new IllegalStateException("Type signature mismatch for " + userType.getSimpleName() +
            ". Expected 0x" + Long.toHexString(typeSignature) +
            " but got 0x" + Long.toHexString(incomingSignature));
      }
    }

    return readFromWire(buffer);
  }

  /// Package-private deserialization method that assumes the type signature has already been read
  /// and validated by the caller (e.g., RefValueReader)
  T deserializeWithoutSignature(ByteBuffer buffer) {
    Objects.requireNonNull(buffer);
    buffer.order(ByteOrder.BIG_ENDIAN);
    return readFromWire(buffer);
  }

  T readFromWire(ByteBuffer buffer) {
    final int countPosition = buffer.position();
    final int wireCount = ZigZagEncoding.getInt(buffer);
    final int afterCountPosition = buffer.position();

    final int componentsToRead = Math.min(wireCount, readers.length);
    Object[] components = new Object[readers.length];
    IntStream.range(0, componentsToRead).forEach(i -> components[i] = readers[i].apply(buffer));

    String compatibilityLog = "";
    if (wireCount < readers.length) {
      if (!compatibilityMode) {
        throw new IllegalStateException(String.format("Wire count %d is less than expected %d and compatibility mode is disabled for %s",
            wireCount, readers.length, userType.getSimpleName()));
      }
      compatibilityLog = String.format(" (filled %d default values)", readers.length - wireCount);
      IntStream.range(wireCount, readers.length).forEach(i -> {
        final RecordComponent rc = userType.getRecordComponents()[i];
        components[i] = Companion.defaultValue(rc.getType());
      });
    }

    final int endPosition = buffer.position();
    final String finalCompatibilityLog = compatibilityLog;
    LOGGER.fine(() -> String.format("[%s.readFromWire] Read count: %d @%d. Read %d components from @%d to @%d%s. Components: %s",
        userType.getSimpleName(), wireCount, countPosition, componentsToRead, afterCountPosition, endPosition,
        finalCompatibilityLog, Arrays.toString(components)));

    // Invoke constructor
    try {
      @SuppressWarnings("unchecked") final var result = (T) this.recordConstructor.invokeWithArguments(components);
      return result;
    } catch (Throwable e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  @Override
  public int maxSizeOf(T object) {
    Objects.requireNonNull(object);
    if (!userType.isAssignableFrom(object.getClass())) {
      throw new IllegalArgumentException("Expected " + userType + " but got " + object.getClass());
    }

    // type signature is fixed size the count of components is fixed size followed by the size of each component
    return Long.BYTES + Integer.BYTES + Arrays.stream(sizers)
        .mapToInt(sizer -> sizer.applyAsInt(object))
        .sum();
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
    return "RecordSerde{userType=" + userType + ", typeSignature=0x" + Long.toHexString(typeSignature) +
        " altTypeSignature=" + altTypeSignature.map(aLong -> "0x" + Long.toHexString(aLong)).orElse("null") + "}";
  }

}
