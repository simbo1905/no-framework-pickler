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
final class RecordSerde2<T> implements Pickler2<T> {
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

  RecordSerde2(Class<?> userType,
               long typeSignature,
               Optional<Long> altTypeSignature
  ) {
    this(userType, typeSignature, altTypeSignature, null, null, null);
  }

  RecordSerde2(Class<?> userType,
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
    final var componentSerdes = Companion2.buildComponentSerdes(
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
      }
    }

    LOGGER.finer(() -> "RecordPickler deserializing record " + this.userType.getSimpleName() + " buffer remaining bytes: " + buffer.remaining() + " limit: " + buffer.limit() + " capacity: " + buffer.capacity());
    return readFromWire(buffer);
  }

  T readFromWire(ByteBuffer buffer) {
    LOGGER.fine(() -> String.format("[%s.readFromWire] Entry. Position: %d", userType.getSimpleName(), buffer.position()));

    final int countPosition = buffer.position();
    LOGGER.finer(() -> String.format("[%s.readFromWire] Reading component count at position %d", userType.getSimpleName(), countPosition));
    final int wireCount = ZigZagEncoding.getInt(buffer);
    LOGGER.fine(() -> String.format("[%s.readFromWire] Read component count. Position before: %d, after: %d. Count: %d",
        userType.getSimpleName(), countPosition, buffer.position(), wireCount));
    // Fill the components from the buffer up to the wireCount
    Object[] components = new Object[readers.length];
    LOGGER.finer(() -> String.format("[%s.readFromWire] Starting to read components", userType.getSimpleName()));

    IntStream.range(0, readers.length).forEach(i -> {
      final int componentIndex = i;
      final int readPosition = buffer.position();
      LOGGER.finer(() -> String.format("[%s.readFromWire] Reading component %d of %d at position %d",
          userType.getSimpleName(), componentIndex + 1, readers.length, readPosition));

      LOGGER.fine(() -> String.format("[%s.readFromWire] Reading component %d at position %d. Reader: %s",
          userType.getSimpleName(), componentIndex, readPosition, readers[componentIndex].getClass().getSimpleName()));

      components[i] = readers[componentIndex].apply(buffer);

      LOGGER.finer(() -> String.format("[%s.readFromWire] Read component %d. Position after: %d. Value: %s",
          userType.getSimpleName(), componentIndex, buffer.position(), components[i]));
    });

    // If we need more, and we are in backwards compatibility mode, fill the remaining components with default values
    if (wireCount < readers.length) {
      LOGGER.fine(() -> "RecordPickler " + userType.getSimpleName() + "wireCount(" + wireCount + ") < componentReaders.length(" +
          readers.length + "). Filling remaining with default values.");
      IntStream.range(wireCount, readers.length).forEach(i -> {
        final RecordComponent rc = userType.getRecordComponents()[i];
        LOGGER.fine(() -> "Filling component " + i + " with default value for type: " + rc.getType().getName());
        components[i] = defaultValue(rc.getType());
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
  Sizer resolveTypeSizer(Class<?> targetClass) {
    return obj -> {
      if (obj == null) return Byte.BYTES;
      if (userType.isAssignableFrom(targetClass)) {
        return this.maxSizeOf((T) obj);
      }
      throw new UnsupportedOperationException("Unhandled type: " + targetClass);
    };
  }

  @SuppressWarnings("unchecked")
  Writer resolveTypeWriter(Class<?> targetClass) {
    return (buffer, obj) -> {
      if (userType.isAssignableFrom(targetClass)) {
        this.writeToWire(buffer, (T) obj);
      } else {
        throw new UnsupportedOperationException("Unhandled type: " + targetClass);
      }
    };
  }

  Reader resolveTypeReader(Long typeSignature) {
    return buffer -> {
      if (typeSignature == 0L) return null;
      if (typeSignature == this.typeSignature) {
        return this.deserialize(buffer);
      }
      throw new UnsupportedOperationException("Unhandled type signature: " + typeSignature);
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
