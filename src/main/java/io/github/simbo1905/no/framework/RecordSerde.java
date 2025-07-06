// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.jetbrains.annotations.NotNull;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Companion.*;

/// Simplified record serialization handler focused on method handles and delegation
final class RecordSerde<T> implements Pickler<T> {
  final Class<?> userType;
  final long typeSignature;
  final MethodHandle recordConstructor;
  final MethodHandle[] componentAccessors;
  final BiConsumer<ByteBuffer, Object>[] componentWriters;
  final Function<ByteBuffer, Object>[] componentReaders;
  final ToIntFunction<Object>[] componentSizers;

  RecordSerde(@NotNull Class<?> userType, Function<Class<?>, ToIntFunction<Object>> sizerResolver,
              Function<Class<?>, BiConsumer<ByteBuffer, Object>> writerResolver,
              Function<Long, Function<ByteBuffer, Object>> readerResolver) {
    assert userType.isRecord() : "User type must be a record: " + userType;
    this.userType = userType;

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

    this.componentAccessors = Arrays.stream(components)
        .map(component -> {
          try {
            return MethodHandles.lookup().unreflect(component.getAccessor());
          } catch (Exception e) {
            throw new RuntimeException("Failed to create accessor for " + component.getName(), e);
          }
        })
        .toArray(MethodHandle[]::new);

    // Compute type signature
    final TypeExpr[] componentTypeExpressions = Arrays.stream(components)
        .map(comp -> TypeExpr.analyzeType(comp.getGenericType()))
        .toArray(TypeExpr[]::new);
    this.typeSignature = hashClassSignature(userType, components, componentTypeExpressions);

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

    LOGGER.fine(() -> "RecordSerde " + userType.getSimpleName() + " construction complete");
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
    LOGGER.fine(() -> "RecordSerde " + userType.getSimpleName() + " writing " + Long.toHexString(typeSignature) + " at position " + buffer.position());

    buffer.putLong(typeSignature);

    IntStream.range(0, componentWriters.length)
        .forEach(i -> {
          componentWriters[i].accept(buffer, record);
        });

    return buffer.position() - startPosition;
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

    return readFromWire(buffer);
  }

  T readFromWire(ByteBuffer buffer) {
    final Object[] components = Arrays.stream(componentReaders).map(componentReader -> componentReader.apply(buffer))
        .toArray();

    try {
      @SuppressWarnings("unchecked") final var result = (T) recordConstructor.invokeWithArguments(components);
      return result;
    } catch (Throwable e) {
      throw new RuntimeException("Failed to construct " + userType, e);
    }
  }

  @Override
  public int maxSizeOf(T object) {
    Objects.requireNonNull(object);
    if (!userType.isAssignableFrom(object.getClass())) {
      throw new IllegalArgumentException("Expected " + userType + " but got " + object.getClass());
    }

    return CLASS_SIG_BYTES + Arrays.stream(componentSizers)
        .mapToInt(sizer -> sizer.applyAsInt(object))
        .sum();
  }

  @Override
  public String toString() {
    return "RecordSerde{userType=" + userType + ", typeSignature=0x" + Long.toHexString(typeSignature) + "}";
  }
}
