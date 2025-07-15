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
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Companion.*;

/// Simplified record serialization handler focused on method handles and delegation
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
final class RecordSerde<T> implements Pickler<T> {
  final Class<?> userType;
  final long typeSignature; // Type signature is a hash of the class name and component metadata
  final Optional<Long> altTypeSignature; // Optional alternative type signature for backwards compatibility
  final MethodHandle recordConstructor;
  final MethodHandle[] componentAccessors;
  final BiConsumer<ByteBuffer, Object>[] componentWriters;
  final Function<ByteBuffer, Object>[] componentReaders;
  final ToIntFunction<Object>[] componentSizers;
  final Class<?>[] componentTypes; // Component types for the record, used for backwards compatibility defaults
  final boolean compatibilityMode;

  RecordSerde(Class<?> userType,
              Function<Class<?>, ToIntFunction<Object>> sizerResolver,
              Function<Class<?>, BiConsumer<ByteBuffer, Object>> writerResolver,
              Function<Long, Function<ByteBuffer, Object>> readerResolver,
              Optional<Long> altTypeSignature
  ) {
    assert userType.isRecord() : "User type must be a record: " + userType;
    this.userType = userType;
    this.altTypeSignature = altTypeSignature;
    compatibilityMode = CompatibilityMode.current() == CompatibilityMode.ENABLED;

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

    // Compute type signature
    final TypeExpr[] componentTypeExpressions = Arrays.stream(components)
        .map(comp -> TypeExpr.analyzeType(comp.getGenericType()))
        .toArray(TypeExpr[]::new);

    typeSignature = hashClassSignature(userType, components, componentTypeExpressions);

    // Build handlers using Companion static methods with callbacks
    final int numComponents = components.length;
    //noinspection RedundantSuppression
    @SuppressWarnings({"unchecked", "rawtypes"}) final BiConsumer<ByteBuffer, Object>[] writers = new BiConsumer[numComponents];
    componentWriters = writers;
    //noinspection RedundantSuppression
    @SuppressWarnings({"unchecked", "rawtypes"}) final Function<ByteBuffer, Object>[] readers = new Function[numComponents];
    componentReaders = readers;
    //noinspection RedundantSuppression
    @SuppressWarnings({"unchecked", "rawtypes"}) final ToIntFunction<Object>[] sizers = new ToIntFunction[numComponents];
    componentSizers = sizers;

    IntStream.range(0, numComponents).forEach(i -> {
      final TypeExpr typeExpr = componentTypeExpressions[i];
      final MethodHandle accessor = componentAccessors[i];

      componentReaders[i] = buildReaderChain(typeExpr, readerResolver);
      componentSizers[i] = buildSizerChain(typeExpr, accessor, sizerResolver);
      componentWriters[i] = buildWriterChain(typeExpr, accessor, writerResolver);
    });

    LOGGER.fine(() -> "RecordSerde " + userType.getSimpleName() + " construction complete with compatibility mode " + compatibilityMode +
        ", type signature 0x" + Long.toHexString(typeSignature) +
        ", alt type signature 0x" + altTypeSignature.map(Long::toHexString).orElse("none") +
        ", and " + componentWriters.length + " components.");
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
    LOGGER.fine(() -> "RecordSerde " + userType.getSimpleName() + " writing typeSignature " + Long.toHexString(typeSignature) + " and component count " + componentWriters.length + " at position " + buffer.position());

    buffer.putLong(typeSignature);
    ZigZagEncoding.putInt(buffer, componentWriters.length);

    IntStream.range(0, componentWriters.length)
        .forEach(i -> componentWriters[i].accept(buffer, record));

    return buffer.position() - startPosition;
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

    if (incomingSignature != this.typeSignature) {
      if (compatibilityMode && altTypeSignature.isPresent() && incomingSignature == altTypeSignature.get()) {
        LOGGER.info(() -> "Type signature mismatch, but compatibility mode is ENABLED. Proceeding with deserialization.");
      }
    } else {
      throw new IllegalStateException("Type signature mismatch: expected " +
          Long.toHexString(this.typeSignature) + " or " +
          altTypeSignature.map(Long::toHexString).orElse("none") + " but got " +
          Long.toHexString(incomingSignature) + " at position: " + typeSigPosition);
    }

    LOGGER.finer(() -> "RecordPickler deserializing record " + this.userType.getSimpleName() + " buffer remaining bytes: " + buffer.remaining() + " limit: " + buffer.limit() + " capacity: " + buffer.capacity());
    return readFromWire(buffer);
  }

  T readFromWire(ByteBuffer buffer) {
    LOGGER.fine(() -> "RecordPickler " + userType.getSimpleName() + " readFromWire() reading component count " + buffer.remaining() + " limit: " + buffer.limit() + " capacity: " + buffer.capacity());
    final int wireCount = ZigZagEncoding.getInt(buffer);

    if (!compatibilityMode && wireCount < componentReaders.length) {
      throw new IllegalStateException("wireCount(" + wireCount + ") < componentReaders.length(" + componentReaders.length + ") and compatibility mode is disabled.");
    } else if (wireCount < 0) {
      throw new IllegalStateException("Invalid wire count: " + wireCount + ". Must be non-negative.");
    } else if (wireCount > componentReaders.length) {
      throw new IllegalStateException("wireCount(" + wireCount + ") > componentReaders.length(" + componentReaders.length + "). This indicates a version mismatch or corrupted data.");
    }

    // Fill the components from the buffer up to the wireCount
    Object[] components = new Object[componentReaders.length];
    IntStream.range(0, wireCount).forEach(i -> {
      final int componentIndex = i; // final for lambda capture
      final int beforePosition = buffer.position();
      LOGGER.fine(() -> "RecordPickler reading component " + componentIndex + " at position " + beforePosition + " buffer remaining bytes: " + buffer.remaining() + " limit: " + buffer.limit() + " capacity: " + buffer.capacity());
      components[i] = componentReaders[i].apply(buffer);
      final Object componentValue = components[i]; // final for lambda capture
      final int afterPosition = buffer.position();
      LOGGER.finer(() -> "Read component " + componentIndex + ": " + componentValue + " moved from position " + beforePosition + " to " + afterPosition);
    });

    // If we need more, and we are in backwards compatibility mode, fill the remaining components with default values
    if (wireCount < componentReaders.length) {
      LOGGER.fine(() -> "RecordPickler " + userType.getSimpleName() + "wireCount(" + wireCount + ") < componentReaders.length(" + componentReaders.length + "). Filling remaining with default values.");
      IntStream.range(wireCount, componentReaders.length).forEach(i -> {
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
    return Long.BYTES + Integer.BYTES + Arrays.stream(componentSizers)
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
