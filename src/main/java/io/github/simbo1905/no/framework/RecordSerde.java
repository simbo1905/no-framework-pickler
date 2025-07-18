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
import java.util.List;
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
  final Sizer[] sizers;
  final Writer[] writers;
  final Reader[] readers;

  RecordSerde(Class<?> userType,
              long typeSignature,
              Optional<Long> altTypeSignature
  ) {
    this(userType, typeSignature, altTypeSignature, null, null, null);
  }

  RecordSerde(Class<?> userType,
              long typeSignature,
              Optional<Long> altTypeSignature,
              SizerResolver sizerResolver,
              WriterResolver writerResolver,
              ReaderResolver readerResolver
  ) {
    assert userType.isRecord() : "User type must be a record: " + userType;
    compatibilityMode = CompatibilityMode.current() == CompatibilityMode.ENABLED;
    this.userType = userType;
    this.typeSignature = typeSignature;
    this.altTypeSignature = altTypeSignature;

    // Create component serdes directly without external resolvers
    final var customHandlers = List.<SerdeHandler>of();
    final var componentSerdes = Companion.buildComponentSerdes(
        userType,
        customHandlers,
        sizerResolver != null ? sizerResolver : this::resolveTypeSizer,
        writerResolver != null ? writerResolver : this::resolveTypeWriter,
        readerResolver != null ? readerResolver : this::resolveTypeReader
    );

    this.sizers = Arrays.stream(componentSerdes).map(ComponentSerde::sizer).toArray(Sizer[]::new);
    this.writers = Arrays.stream(componentSerdes).map(ComponentSerde::writer).toArray(Writer[]::new);
    this.readers = Arrays.stream(componentSerdes).map(ComponentSerde::reader).toArray(Reader[]::new);

    final RecordComponent[] components = userType.getRecordComponents();
    assert components != null && components.length > 0 : "Record must have components: " + userType;

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

    //noinspection RedundantSuppression
    @SuppressWarnings("rawtypes") final var raw = (Class<?>[]) new Class[componentAccessors.length];
    componentTypes = raw;
    IntStream.range(0, componentAccessors.length).forEach(i -> {
      final RecordComponent rc = components[i];
      componentTypes[i] = rc.getType();
    });

    LOGGER.fine(() -> "RecordSerde " + userType.getSimpleName() + " construction complete with compatibility mode " + compatibilityMode +
        ", type signature 0x" + Long.toHexString(typeSignature) +
        ", alt type signature 0x" + altTypeSignature.map(Long::toHexString).orElse("none"));
  }

  @Override
  public int serialize(ByteBuffer buffer, T record) {
    Objects.requireNonNull(buffer);
    Objects.requireNonNull(record);
    buffer.order(ByteOrder.BIG_ENDIAN);
    if (!userType.isAssignableFrom(record.getClass())) {
      throw new IllegalArgumentException("Expected " + userType + " but got " + record.getClass());
    }

    LOGGER.fine(() -> "RecordSerde " + userType.getSimpleName() + " serialize " + record.hashCode() + " at position " + buffer.position());
    return writeToWire(buffer, record);
  }

  int writeToWire(ByteBuffer buffer, T record) {
    final int startPosition = buffer.position();
    LOGGER.fine(() -> String.format("writeToWire START at position %d", startPosition));

    LOGGER.finer(() -> String.format("Writing type signature: 0x%s at position %d",
        Long.toHexString(typeSignature), startPosition));
    buffer.putLong(typeSignature);
    final int afterSigPosition = buffer.position();
    LOGGER.fine(() -> String.format("Wrote type signature, position now: %d", afterSigPosition));

    LOGGER.finer(() -> String.format("Writing component count: %d at position %d",
        writers.length, afterSigPosition));
    ZigZagEncoding.putInt(buffer, writers.length);
    final int afterCountPosition = buffer.position();
    LOGGER.fine(() -> String.format("Wrote component count %d, position now: %d",
        writers.length, afterCountPosition));

    // Log component writing details
    IntStream.range(0, writers.length)
        .forEach(i -> {
          final int posBefore = buffer.position();
          LOGGER.finer(() -> String.format("Starting component %d of %d at position %d",
              i + 1, writers.length, posBefore));
          LOGGER.fine(() -> String.format("Writing component %d at position %d", i, posBefore));
          writers[i].accept(buffer, record);
          final int posAfter = buffer.position();
          LOGGER.finer(() -> String.format("Finished component %d at position %d (wrote %d bytes)",
              i, posAfter, posAfter - posBefore));
        });

    final int endPosition = buffer.position();
    LOGGER.fine(() -> String.format("writeToWire END for %s. Total bytes written: %d (from %d to %d)",
        userType.getSimpleName(), (endPosition - startPosition), startPosition, endPosition));
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
    // read the type signature first as it is a cryptographic hash of the class name and component metadata and fixed size
    final long incomingSignature = buffer.getLong();
    LOGGER.fine(() -> "RecordPickler " + userType.getSimpleName() + " deserialize() read type signature 0x" + Long.toHexString(incomingSignature) + " at position " + typeSigPosition + ", expected 0x" + Long.toHexString(typeSignature));

    LOGGER.fine(() -> "Signature comparison: (incomingSignature != this.typeSignature) is " + (incomingSignature != this.typeSignature));

    if (incomingSignature != this.typeSignature) {
      if (compatibilityMode && altTypeSignature.isPresent() && incomingSignature == altTypeSignature.get()) {
        LOGGER.info(() -> "Type signature mismatch, but compatibility mode is ENABLED. Proceeding with deserialization.");
      } else {
        throw new IllegalStateException("Type signature mismatch for " + userType.getSimpleName() +
            ". Expected 0x" + Long.toHexString(typeSignature) +
            " but got 0x" + Long.toHexString(incomingSignature));
      }
    }

    LOGGER.finer(() -> "RecordPickler deserializing record " + this.userType.getSimpleName() + " buffer remaining bytes: " + buffer.remaining() + " limit: " + buffer.limit() + " capacity: " + buffer.capacity());
    return readFromWire(buffer);
  }

  /// Package-private deserialization method that assumes the type signature has already been read
  /// and validated by the caller (e.g., RefValueReader)
  T deserializeWithoutSignature(ByteBuffer buffer) {
    Objects.requireNonNull(buffer);
    buffer.order(ByteOrder.BIG_ENDIAN);
    LOGGER.fine(() -> "RecordPickler " + userType.getSimpleName() +
        " deserializeWithoutSignature() at position " + buffer.position());
    return readFromWire(buffer);
  }

  T readFromWire(ByteBuffer buffer) {
    LOGGER.fine(() -> String.format("[%s.readFromWire] Entry. Position: %d/%d with %d bytes remaining",
        userType.getSimpleName(), buffer.position(), buffer.limit(), buffer.remaining()));

    final int countPosition = buffer.position();
    final int bufferLimit = buffer.limit();
    final int bufferRemaining = buffer.remaining();
    LOGGER.finer(() -> String.format("[%s.readFromWire] Reading component count at position %d/%d with %d bytes remaining",
        userType.getSimpleName(), countPosition, bufferLimit, bufferRemaining));

    final int wireCount = ZigZagEncoding.getInt(buffer);
    LOGGER.fine(() -> String.format("[%s.readFromWire] Read component count. Position before: %d, after: %d. Count: %d",
        userType.getSimpleName(), countPosition, buffer.position(), wireCount));

    LOGGER.finer(() -> String.format("[%s.readFromWire] Component count validation: wireCount=%d, readers.length=%d, compatibilityMode=%b",
        userType.getSimpleName(), wireCount, readers.length, compatibilityMode));
    // Fill the components from the buffer up to the wireCount
    Object[] components = new Object[readers.length];
    LOGGER.finer(() -> String.format("[%s.readFromWire] Starting to read components", userType.getSimpleName()));

    LOGGER.finer(() -> String.format("[%s.readFromWire] Starting to read %d components at position %d/%d",
        userType.getSimpleName(), readers.length, buffer.position(), buffer.limit()));

    IntStream.range(0, readers.length).forEach(i -> {
      final int componentIndex = i;
      final int readPosition = buffer.position();
      final int readLimit = buffer.limit();
      final int readRemaining = buffer.remaining();
      LOGGER.finer(() -> String.format("[%s.readFromWire] Reading component %d of %d at position %d/%d with %d bytes remaining",
          userType.getSimpleName(), componentIndex + 1, readers.length, readPosition, readLimit, readRemaining));

      LOGGER.fine(() -> String.format("[%s.readFromWire] Reading component %d at position %d. Reader: %s",
          userType.getSimpleName(), componentIndex, readPosition, readers[componentIndex].getClass().getSimpleName()));

      components[i] = readers[componentIndex].apply(buffer);

      LOGGER.finer(() -> String.format("[%s.readFromWire] Read component %d. Position after: %d. Value: %s",
          userType.getSimpleName(), componentIndex, buffer.position(), components[i]));

      if (componentIndex + 1 < readers.length) {
        LOGGER.finer(() -> String.format("[%s.readFromWire] Moving to next component at position %d",
            userType.getSimpleName(), buffer.position()));
      }
    });

    // If we need more, and we are in backwards compatibility mode, fill the remaining components with default values
    if (wireCount < readers.length) {
      LOGGER.fine(() -> String.format("%s.readFromWire] Need to fill %d components due to wireCount=%d < readers.length=%d",
          userType.getSimpleName(), readers.length - wireCount, wireCount, readers.length));

      IntStream.range(wireCount, readers.length).forEach(i -> {
        final RecordComponent rc = userType.getRecordComponents()[i];
        final Object defaultValue = defaultValue(rc.getType());
        LOGGER.finer(() -> String.format("%s.readFromWire] Filling component %d of %d with default value of type %s: %s",
            userType.getSimpleName(), i + 1, readers.length, rc.getType().getName(), defaultValue));
        components[i] = defaultValue;
      });
    }

    // Invoke constructor
    try {
      LOGGER.finer(() -> "Constructing record at position " + buffer.position() + " with components: " + Arrays.toString(components));
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
    return "RecordSerde{userType=" + userType + ", typeSignature=0x" + Long.toHexString(typeSignature) + "}";
  }

  @SuppressWarnings("unchecked")
  /// Sizer provides worst-case size estimates without performing computations for compression
  /// FIXME try to hoist any such logic up to the factory method Pickler.forClass and delegate to that only
  Sizer resolveTypeSizer(Class<?> targetClass) {
    return obj -> {
      if (obj == null) return Byte.BYTES;
      else if (userType.isAssignableFrom(targetClass)) {
        return this.maxSizeOf((T) obj);
      }
      throw new UnsupportedOperationException("Unhandled type: " + targetClass);
    };
  }

  @SuppressWarnings("unchecked")
  /// Writer handles serialization including compression computations for wire format
  /// FIXME try to hoist any such logic up to the factory method Pickler.forClass and delegate to that only
  Writer resolveTypeWriter(Class<?> targetClass) {
    return (buffer, obj) -> {
      if (userType.isAssignableFrom(targetClass)) {
        this.writeToWire(buffer, (T) obj);
      } else {
        throw new UnsupportedOperationException("Unhandled type: " + targetClass);
      }
    };
  }

  /// Reader handles deserialization including decompression computations from wire format
  /// FIXME try to hoist any such logic up to the factory method Pickler.forClass and delegate to that only
  Reader resolveTypeReader(Long typeSignature) {
    return buffer -> {
      if (typeSignature == 0L) return null;
      else if (typeSignature == this.typeSignature) {
        // When called via Companion's type resolution, the signature has already been read
        return this.deserializeWithoutSignature(buffer);
      } else {
        throw new UnsupportedOperationException("Type signature not supported by this record serde: " + typeSignature + " (expected: " + this.typeSignature + ")");
      }
    };
  }

  static Object defaultValue(Class<?> type) {
    if (type.isPrimitive()) {
      if (type == boolean.class) {
        return false;
      } else if (type == byte.class) {
        return (byte) 0;
      } else if (type == short.class) {
        return (short) 0;
      } else if (type == int.class) {
        return 0;
      } else if (type == long.class) {
        return 0L;
      } else if (type == float.class) {
        return 0.0f;
      } else if (type == double.class) {
        return 0.0;
      } else if (type == char.class) {
        return '\u0000';
      }
    }
    return null;
  }
}
