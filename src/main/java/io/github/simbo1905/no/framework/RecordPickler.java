// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.jetbrains.annotations.NotNull;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.simbo1905.no.framework.Constants.INTEGER_VAR;
import static io.github.simbo1905.no.framework.PicklerImpl.*;
import static io.github.simbo1905.no.framework.Tag.INTEGER;

final class RecordPickler<T> implements Pickler<T> {
  static final CompatibilityMode COMPATIBILITY_MODE = CompatibilityMode.valueOf(System.getProperty("no.framework.Pickler.Compatibility", "DISABLED"));

  public static final String SHA_256 = "SHA-256";
  static final int SAMPLE_SIZE = 32;
  static final int CLASS_SIG_BYTES = Long.BYTES;
  // Global lookup tables indexed by ordinal - the core of the unified architecture
  final Class<?> userType;
  final Class<?>[] sortedUserTypes; // Sorted user types for ordinal lookup
  final long typeSignature;    // CLASS_SIG_BYTES SHA256 signatures for backwards compatibility checking
  final MethodHandle recordConstructor;      // Constructor direct method handle
  final MethodHandle[] componentAccessors;    // Accessor direct method handle
  final TypeExpr[] componentTypeExpressions;       // Component type AST structure
  final BiConsumer<ByteBuffer, Object>[] componentWriters;  // writer chain of delegating lambda eliminating use of `switch` on write the hot path
  final Function<ByteBuffer, Object>[] componentReaders;    // reader chain of delegating lambda eliminating use of `switch` on read the hot path
  final ToIntFunction<Object>[] componentSizers; // Sizer lambda

  public RecordPickler(
      @NotNull Class<?> userType,
      Class<?>[] sortedUserTypes
  ) {
    this.userType = userType;
    this.sortedUserTypes = sortedUserTypes;

    LOGGER.finer(() -> "RecordPickler construction starting for " + userType.getSimpleName());

    // Get component accessors and analyze types
    RecordComponent[] components = userType.getRecordComponents();
    int numComponents = components.length;

    LOGGER.finer(() -> "Found " + numComponents + " components for " + userType.getSimpleName());

    componentTypeExpressions = new TypeExpr[numComponents];
    //noinspection unchecked
    componentWriters = new BiConsumer[numComponents];
    //noinspection unchecked
    componentReaders = new Function[numComponents];
    //noinspection unchecked
    componentSizers = new ToIntFunction[numComponents];

    IntStream.range(0, numComponents).forEach(i -> {
      RecordComponent component = components[i];
      final TypeExpr typeExpr = TypeExpr.analyzeType(component.getGenericType());
      componentTypeExpressions[i] = typeExpr;
      LOGGER.finer(() -> "Component " + i + " (" + component.getName() + ") has type expression: " + typeExpr.toTreeString());
    });

    // Compute and store the type signature for this record
    typeSignature = hashClassSignature(userType, components, componentTypeExpressions);
    LOGGER.finer(() -> "Computed type signature: 0x" + Long.toHexString(typeSignature) + " for " + userType.getSimpleName());

    final Constructor<?> constructor;
    try {
      Class<?>[] parameterTypes = Arrays.stream(components).map(RecordComponent::getType).toArray(Class<?>[]::new);
      constructor = userType.getDeclaredConstructor(parameterTypes);
      recordConstructor = MethodHandles.lookup().unreflectConstructor(constructor);
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new RuntimeException("Records should be public such as a top level of static nested type: " + e.getMessage(), e);
    }

    componentAccessors = new MethodHandle[numComponents];
    IntStream.range(0, numComponents).forEach(i -> {
      try {
        componentAccessors[i] = MethodHandles.lookup().unreflect(components[i].getAccessor());
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Failed to un reflect accessor for " + components[i].getName(), e);
      }
    });

    LOGGER.fine(() -> "Building code for : " + userType.getSimpleName());
    IntStream.range(0, this.componentAccessors.length).forEach(i -> {
      final var accessor = componentAccessors[i];
      final var typeExpr = componentTypeExpressions[i];
      // Build writer, reader, and sizer chains
      componentWriters[i] = buildWriterChain(typeExpr, accessor);
      componentReaders[i] = buildReaderChain(typeExpr);
      componentSizers[i] = buildSizerChain(typeExpr, accessor);
      LOGGER.finer(() -> "Built writer/reader/sizer chains for component " + i + " with type " + typeExpr.toTreeString());
    });

    LOGGER.info(() -> "RecordPickler construction complete - ready for high-performance serialization");
  }

  public static @NotNull BiConsumer<ByteBuffer, Object> buildPrimitiveArrayWriter(TypeExpr.PrimitiveValueType primitiveType, MethodHandle typeExpr0Accessor) {
    LOGGER.fine(() -> "Building writer chain for primitive array type: " + primitiveType);
    return switch (primitiveType) {
      case BOOLEAN -> (buffer, record) -> {
        final var position = buffer.position();
        final Object value;
        try {
          value = typeExpr0Accessor.invokeWithArguments(record);
        } catch (Throwable e) {
          throw new RuntimeException(e.getMessage(), e);
        }
        final var booleans = (boolean[]) value;
        ZigZagEncoding.putInt(buffer, Constants.BOOLEAN.marker());
        int length = booleans.length;
        ZigZagEncoding.putInt(buffer, length);
        BitSet bitSet = new BitSet(length);
        // Create a BitSet and flip bits to try where necessary
        IntStream.range(0, length).filter(i -> booleans[i]).forEach(bitSet::set);
        byte[] bytes = bitSet.toByteArray();
        ZigZagEncoding.putInt(buffer, bytes.length);
        buffer.put(bytes);
        LOGGER.finer(() -> "Written primitive ARRAY for tag BOOLEAN at position "
            + position + " with length=" + length + " and bytes length=" + bytes.length);
      };
      case BYTE -> (buffer, record) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag BYTE at position " + buffer.position());
        final Object value;
        try {
          value = typeExpr0Accessor.invokeWithArguments(record);
        } catch (Throwable e) {
          throw new RuntimeException(e.getMessage(), e);
        }
        final var bytes = (byte[]) value;
        ZigZagEncoding.putInt(buffer, Constants.BYTE.marker());
        ZigZagEncoding.putInt(buffer, bytes.length);
        buffer.put(bytes);
      };
      case SHORT -> (buffer, record) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag SHORT at position " + buffer.position());
        final Object value;
        try {
          value = typeExpr0Accessor.invokeWithArguments(record);
        } catch (Throwable e) {
          throw new RuntimeException(e.getMessage(), e);
        }
        ZigZagEncoding.putInt(buffer, Constants.SHORT.marker());
        final var shorts = (short[]) value;
        ZigZagEncoding.putInt(buffer, shorts.length);
        for (short s : shorts) {
          buffer.putShort(s);
        }
      };
      case CHARACTER -> (buffer, record) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag CHARACTER at position " + buffer.position());
        final Object value;
        try {
          value = typeExpr0Accessor.invokeWithArguments(record);
        } catch (Throwable e) {
          throw new RuntimeException(e.getMessage(), e);
        }
        final var chars = (char[]) value;
        ZigZagEncoding.putInt(buffer, Constants.CHARACTER.marker());
        ZigZagEncoding.putInt(buffer, chars.length);
        for (char c : chars) {
          buffer.putChar(c);
        }
      };
      case FLOAT -> (buffer, record) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag FLOAT at position " + buffer.position());
        final Object value;
        try {
          value = typeExpr0Accessor.invokeWithArguments(record);
        } catch (Throwable e) {
          throw new RuntimeException(e.getMessage(), e);
        }
        final var floats = (float[]) value;
        ZigZagEncoding.putInt(buffer, Constants.FLOAT.marker());
        ZigZagEncoding.putInt(buffer, floats.length);
        for (float f : floats) {
          buffer.putFloat(f);
        }
      };
      case DOUBLE -> (buffer, record) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag DOUBLE at position " + buffer.position());
        final Object value;
        try {
          value = typeExpr0Accessor.invokeWithArguments(record);
        } catch (Throwable e) {
          throw new RuntimeException(e.getMessage(), e);
        }
        ZigZagEncoding.putInt(buffer, Constants.DOUBLE.marker());
        final var doubles = (double[]) value;
        ZigZagEncoding.putInt(buffer, doubles.length);
        for (double d : doubles) {
          buffer.putDouble(d);
        }
      };
      case INTEGER -> (buffer, record) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag INTEGER at position " + buffer.position());
        final Object value;
        try {
          value = typeExpr0Accessor.invokeWithArguments(record);
        } catch (Throwable e) {
          throw new RuntimeException(e.getMessage(), e);
        }
        final var integers = (int[]) value;
        final var length = Array.getLength(value);
        final var sampleAverageSize = length > 0 ? estimateAverageSizeInt(integers, length) : 1;
        // Here we must be saving one byte per integer to justify the encoding cost
        if (sampleAverageSize < Integer.BYTES - 1) {
          LOGGER.finer(() -> "Delegating ARRAY for tag " + INTEGER_VAR + " with length=" + Array.getLength(value) + " at position " + buffer.position());
          ZigZagEncoding.putInt(buffer, Constants.INTEGER_VAR.marker());
          ZigZagEncoding.putInt(buffer, length);
          for (int i : integers) {
            ZigZagEncoding.putInt(buffer, i);
          }
        } else {
          LOGGER.finer(() -> "Delegating ARRAY for tag " + INTEGER + " with length=" + Array.getLength(value) + " at position " + buffer.position());
          ZigZagEncoding.putInt(buffer, Constants.INTEGER.marker());
          ZigZagEncoding.putInt(buffer, length);
          for (int i : integers) {
            buffer.putInt(i);
          }
        }
      };
      case LONG -> (buffer, record) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag LONG at position " + buffer.position());
        final Object value;
        try {
          value = typeExpr0Accessor.invokeWithArguments(record);
        } catch (Throwable e) {
          throw new RuntimeException(e.getMessage(), e);
        }
        final var longs = (long[]) value;
        final var length = Array.getLength(value);
        final var sampleAverageSize = length > 0 ? estimateAverageSizeLong(longs, length) : 1;
        if ((length <= SAMPLE_SIZE && sampleAverageSize < Long.BYTES - 1) ||
            (length > SAMPLE_SIZE && sampleAverageSize < Long.BYTES - 2)) {
          LOGGER.fine(() -> "Writing LONG_VAR array - position=" + buffer.position() + " length=" + length);
          ZigZagEncoding.putInt(buffer, Constants.LONG_VAR.marker());
          ZigZagEncoding.putInt(buffer, length);
          for (long i : longs) {
            ZigZagEncoding.putLong(buffer, i);
          }
        } else {
          LOGGER.fine(() -> "Writing LONG array - position=" + buffer.position() + " length=" + length);
          ZigZagEncoding.putInt(buffer, Constants.LONG.marker());
          ZigZagEncoding.putInt(buffer, length);
          for (long i : longs) {
            buffer.putLong(i);
          }
        }
      };
    };
  }

  BiConsumer<ByteBuffer, Object> buildWriterChain(TypeExpr typeExpr, MethodHandle methodHandle) {
    if (typeExpr.isPrimitive()) {
      // For primitive types, we can directly write the value using the method handle
      LOGGER.fine(() -> "Building writer chain for primitive type: " + typeExpr.toTreeString());
      final var primitiveType = ((TypeExpr.PrimitiveValueNode) typeExpr).type();
      return buildPrimitiveValueWriter(primitiveType, methodHandle);
    } else if (typeExpr.isRecord()) {
      final TypeExpr.RefValueNode refValueNode = (TypeExpr.RefValueNode) typeExpr;
      final var recordJavaType = refValueNode.javaType();
      if (recordJavaType instanceof Class<?> clz && this.userType.isAssignableFrom(clz)) {
        // self picker
        LOGGER.fine(() -> "Building writer chain for self type: " + typeExpr.toTreeString() + " with method handle: " + methodHandle);
        return (ByteBuffer buffer, Object record) -> {
          if (record.getClass() != userType) {
            throw new IllegalArgumentException("Record type mismatch: expected " + userType.getSimpleName() + " but got " + record.getClass().getSimpleName());
          }
          final Object inner;
          try {
            inner = methodHandle.invokeWithArguments(record);
          } catch (Throwable e) {
            throw new RuntimeException("Failed to write boolean value", e);
          }
          if (inner == null) {
            // If the inner value is null, we write a zero type signature for null
            buffer.putLong(0L);
          } else
            //noinspection unchecked
            this.writeToWire(buffer, (T) inner);
        };
      }
      return (ByteBuffer buffer, Object record) -> {
        LOGGER.fine(() -> "Building writer chain for record type: " + typeExpr.toTreeString() + " with method handle: " + methodHandle);
        throw new AssertionError("not implemented: " + typeExpr.toTreeString() + " for record: " + record.getClass().getSimpleName() + " with method handle: " + methodHandle);
      };
    } else {
      return (ByteBuffer buffer, Object record) -> {
        throw new AssertionError("not implemented: " + typeExpr.toTreeString() + " for record: " + record.getClass().getSimpleName() + " with method handle: " + methodHandle);
      };
    }
  }

  Function<ByteBuffer, Object> buildReaderChain(TypeExpr typeExpr) {
    if (typeExpr.isPrimitive()) {
      LOGGER.fine(() -> "Building reader chain for primitive type: " + typeExpr.toTreeString());
      final var primitiveType = ((TypeExpr.PrimitiveValueNode) typeExpr).type();
      return buildPrimitiveValueReader(primitiveType);
    } else {
      final TypeExpr.RefValueNode refValueNode = (TypeExpr.RefValueNode) typeExpr;
      final var recordJavaType = refValueNode.javaType();
      if (recordJavaType instanceof Class<?> clz && this.userType.isAssignableFrom(clz)) {
        // self picker
        LOGGER.fine(() -> "Building reader chain for self type: " + typeExpr.toTreeString());
        return (ByteBuffer buffer) -> {
          final long typeSignature = buffer.getLong();
          if (typeSignature == 0L) {
            // If the type signature is zero, we return null
            return null;
          }
          if (typeSignature != this.typeSignature) {
            throw new IllegalStateException("Type signature mismatch: expected " +
                Long.toHexString(this.typeSignature) + " but got " +
                Long.toHexString(typeSignature));
          }
          Object[] args = new Object[componentAccessors.length];
          IntStream.range(0, componentAccessors.length).forEach(i -> {
            args[i] = componentReaders[i].apply(buffer);
          });
          // Read the record using the record constructor
          try {
            return recordConstructor.invokeWithArguments(args);
          } catch (Throwable e) {
            throw new RuntimeException("Failed to construct record of type: " + userType.getSimpleName(), e);
          }
        };
      }
      return (ByteBuffer buffer) -> {
        throw new AssertionError("not implemented: " + typeExpr.toTreeString() + " for record with method handle: ");
      };
    }
  }


  ToIntFunction<Object> buildSizerChain(TypeExpr typeExpr, MethodHandle methodHandle) {
    if (typeExpr.isPrimitive()) {
      LOGGER.fine(() -> "Building sizer chain for primitive type: " + typeExpr.toTreeString());
      final var primitiveType = ((TypeExpr.PrimitiveValueNode) typeExpr).type();
      return buildPrimitiveValueSizer(primitiveType, methodHandle);
    }
    final TypeExpr.RefValueNode refValueNode = (TypeExpr.RefValueNode) typeExpr;
    final var recordJavaType = refValueNode.javaType();
    if (recordJavaType instanceof Class<?> clz && this.userType.isAssignableFrom(clz)) {
      return (Object record) -> {
        final Object inner;
        try {
          inner = methodHandle.invokeWithArguments(record);
        } catch (Throwable t) {
          throw new RuntimeException("Failed to get record component value for sizing: " + t.getMessage(), t);
        }
        if (inner != null) {
          int size = CLASS_SIG_BYTES;
          for (var sizer : componentSizers) {
            size += sizer.applyAsInt(inner); // null is a placeholder, as we don't need the actual record for sizing
          }
          return size;
        } else {
          return Long.BYTES; // null is written as a zero type signature
        }
      };
    }
    throw new AssertionError("not implemented: " + typeExpr.toTreeString() + " for record with method handle: " + methodHandle);
  }

  /// Compute a CLASS_SIG_BYTES signature from class name and component metadata
  static long hashClassSignature(Class<?> clazz, RecordComponent[] components, TypeExpr[] componentTypes) {
    try {
      MessageDigest digest = MessageDigest.getInstance(SHA_256);
      String input = Stream.concat(
              Stream.of(clazz.getSimpleName()),
              IntStream.range(0, components.length).boxed()
                  .flatMap(i -> Stream.concat(Stream.of(componentTypes[i].toTreeString())
                      , Stream.of(components[i].getName()))))
          .collect(Collectors.joining("!"));
      byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
      // Convert first CLASS_SIG_BYTES to long
      //      Byte Index:   0       1       2        3        4        5        6        7
      //      Bits:      [56-63] [48-55] [40-47] [32-39] [24-31] [16-23] [ 8-15] [ 0-7]
      //      Shift:      <<56   <<48   <<40    <<32    <<24    <<16    <<8     <<0
      return IntStream.range(0, CLASS_SIG_BYTES).mapToLong(i -> (hash[i] & 0xFFL) << (56 - i * 8)).reduce(0L, (a, b) -> a | b);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(SHA_256 + " not available", e);
    }
  }

  static @NotNull BiConsumer<ByteBuffer, Object> buildPrimitiveValueWriter(TypeExpr.PrimitiveValueType primitiveType, MethodHandle methodHandle) {
    return switch (primitiveType) {
      case BOOLEAN -> {
        LOGGER.fine(() -> "Building writer chain for boolean.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            final var result = methodHandle.invokeWithArguments(record);
            buffer.put((byte) ((boolean) result ? 1 : 0));
          } catch (Throwable e) {
            throw new RuntimeException("Failed to write boolean value", e);
          }
        };
      }
      case BYTE -> {
        LOGGER.fine(() -> "Building writer chain for byte.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            buffer.put((byte) methodHandle.invokeWithArguments(record));
          } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
          }
        };
      }
      case SHORT -> {
        LOGGER.fine(() -> "Building writer chain for short.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            buffer.putShort((short) methodHandle.invokeWithArguments(record));
          } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
          }
        };
      }
      case CHARACTER -> {
        LOGGER.fine(() -> "Building writer chain for char.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            buffer.putChar((char) methodHandle.invokeWithArguments(record));
          } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
          }
        };
      }
      case INTEGER -> {
        LOGGER.fine(() -> "Building writer chain for int.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            int result = (int) methodHandle.invokeWithArguments(record);
            LOGGER.finer(() -> "INTEGER writer: value=" + result + " position=" + buffer.position() + " zigzag_size=" + ZigZagEncoding.sizeOf(result));
            if (ZigZagEncoding.sizeOf(result) < Integer.BYTES) {
              LOGGER.finer(() -> "Writing INTEGER_VAR marker=" + Constants.INTEGER_VAR.marker() + " at position: " + buffer.position());
              ZigZagEncoding.putInt(buffer, Constants.INTEGER_VAR.marker());
              LOGGER.finer(() -> "Writing INTEGER_VAR value=" + result + " at position: " + buffer.position());
              ZigZagEncoding.putInt(buffer, result);
            } else {
              LOGGER.finer(() -> "Writing INTEGER marker=" + Constants.INTEGER.marker() + " at position: " + buffer.position());
              ZigZagEncoding.putInt(buffer, Constants.INTEGER.marker());
              LOGGER.finer(() -> "Writing INTEGER value=" + result + " at position: " + buffer.position());
              buffer.putInt(result);
            }
          } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
          }
        };
      }
      case LONG -> {
        LOGGER.fine(() -> "Building writer chain for long.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            buffer.putLong((long) methodHandle.invokeWithArguments(record));
          } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
          }
        };
      }
      case FLOAT -> {
        LOGGER.fine(() -> "Building writer chain for float.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            buffer.putFloat((float) methodHandle.invokeWithArguments(record));
          } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
          }
        };
      }
      case DOUBLE -> {
        LOGGER.fine(() -> "Building writer chain for double.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            buffer.putDouble((double) methodHandle.invokeWithArguments(record));
          } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
          }
        };
      }
    };
  }

  static @NotNull Function<ByteBuffer, Object> buildPrimitiveValueReader(TypeExpr.PrimitiveValueType primitiveType) {
    return switch (primitiveType) {
      case BOOLEAN -> (buffer) -> buffer.get() != 0;
      case BYTE -> ByteBuffer::get;
      case SHORT -> ByteBuffer::getShort;
      case CHARACTER -> ByteBuffer::getChar;
      case FLOAT -> ByteBuffer::getFloat;
      case DOUBLE -> ByteBuffer::getDouble;
      case INTEGER -> (buffer) -> {
        final var position = buffer.position();
        LOGGER.finer(() -> "INTEGER reader: starting at position=" + position);
        final int marker = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "INTEGER reader: read marker=" + marker + " INTEGER_VAR.marker=" + Constants.INTEGER_VAR.marker() + " INTEGER.marker=" + Constants.INTEGER.marker() + " at position=" + position);
        if (marker == Constants.INTEGER_VAR.marker()) {
          int value = ZigZagEncoding.getInt(buffer);
          LOGGER.finer(() -> "INTEGER reader: read INTEGER_VAR value=" + value + " at position=" + buffer.position());
          return value;
        } else if (marker == Constants.INTEGER.marker()) {
          int value = buffer.getInt();
          LOGGER.finer(() -> "INTEGER reader: read INTEGER value=" + value + " at position=" + buffer.position());
          return value;
        } else throw new IllegalStateException(
            "Expected INTEGER or INTEGER_VAR marker but got: " + marker + " at position: " + position);
      };
      case LONG -> (buffer) -> {
        final var position = buffer.position();
        final int marker = ZigZagEncoding.getInt(buffer);
        if (marker == Constants.LONG_VAR.marker()) {
          return ZigZagEncoding.getLong(buffer);
        } else if (marker == Constants.LONG.marker()) {
          return buffer.getLong();
        } else throw new IllegalStateException(
            "Expected LONG or LONG_VAR marker but got: " + marker + " at position: " + position);
      };
    };
  }

  static @NotNull Function<ByteBuffer, Object> buildPrimitiveArrayReader(TypeExpr.PrimitiveValueType primitiveType) {
    return switch (primitiveType) {
      case BOOLEAN -> (buffer) -> {
        int marker = ZigZagEncoding.getInt(buffer);
        assert marker == Constants.BOOLEAN.marker() : "Expected BOOLEAN marker but got: " + marker;
        int boolLength = ZigZagEncoding.getInt(buffer);
        boolean[] booleans = new boolean[boolLength];
        int bytesLength = ZigZagEncoding.getInt(buffer);
        byte[] bytes = new byte[bytesLength];
        buffer.get(bytes);
        BitSet bitSet = BitSet.valueOf(bytes);
        IntStream.range(0, boolLength).forEach(i -> booleans[i] = bitSet.get(i));
        return booleans;
      };
      case BYTE -> (buffer) -> {
        int marker = ZigZagEncoding.getInt(buffer);
        assert marker == Constants.BYTE.marker() : "Expected BYTE marker but got: " + marker;
        int length = ZigZagEncoding.getInt(buffer);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
      };
      case SHORT -> (buffer) -> {
        int marker = ZigZagEncoding.getInt(buffer);
        assert marker == Constants.SHORT.marker() : "Expected SHORT marker but got: " + marker;
        int length = ZigZagEncoding.getInt(buffer);
        short[] shorts = new short[length];
        IntStream.range(0, length).forEach(i -> shorts[i] = buffer.getShort());
        return shorts;
      };
      case CHARACTER -> (buffer) -> {
        int marker = ZigZagEncoding.getInt(buffer);
        assert marker == Constants.CHARACTER.marker() : "Expected CHARACTER marker but got: " + marker;
        int length = ZigZagEncoding.getInt(buffer);
        char[] chars = new char[length];
        IntStream.range(0, length).forEach(i -> chars[i] = buffer.getChar());
        return chars;
      };
      case FLOAT -> (buffer) -> {
        int marker = ZigZagEncoding.getInt(buffer);
        assert marker == Constants.FLOAT.marker() : "Expected FLOAT marker but got: " + marker;
        int length = ZigZagEncoding.getInt(buffer);
        float[] floats = new float[length];
        IntStream.range(0, length).forEach(i -> floats[i] = buffer.getFloat());
        return floats;
      };
      case DOUBLE -> (buffer) -> {
        int marker = ZigZagEncoding.getInt(buffer);
        assert marker == Constants.DOUBLE.marker() : "Expected DOUBLE marker but got: " + marker;
        int length = ZigZagEncoding.getInt(buffer);
        double[] doubles = new double[length];
        IntStream.range(0, length).forEach(i -> doubles[i] = buffer.getDouble());
        return doubles;
      };
      case INTEGER -> (buffer) -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker == Constants.INTEGER_VAR.marker()) {
          int length = ZigZagEncoding.getInt(buffer);
          int[] integers = new int[length];
          IntStream.range(0, length).forEach(i -> integers[i] = ZigZagEncoding.getInt(buffer));
          return integers;
        } else if (marker == Constants.INTEGER.marker()) {
          int length = ZigZagEncoding.getInt(buffer);
          int[] integers = new int[length];
          IntStream.range(0, length).forEach(i -> integers[i] = buffer.getInt());
          return integers;
        } else throw new IllegalStateException("Expected INTEGER or INTEGER_VAR marker but got: " + marker);
      };
      case LONG -> (buffer) -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker == Constants.LONG_VAR.marker()) {
          int length = ZigZagEncoding.getInt(buffer);
          long[] longs = new long[length];
          IntStream.range(0, length).forEach(i -> longs[i] = ZigZagEncoding.getLong(buffer));
          return longs;
        } else if (marker == Constants.LONG.marker()) {
          int length = ZigZagEncoding.getInt(buffer);
          long[] longs = new long[length];
          IntStream.range(0, length).forEach(i -> longs[i] = buffer.getLong());
          return longs;
        } else throw new IllegalStateException("Expected LONG or LONG_VAR marker but got: " + marker);
      };
    };
  }

  static @NotNull ToIntFunction<Object> buildPrimitiveValueSizer(TypeExpr.PrimitiveValueType primitiveType, MethodHandle ignored) {
    return switch (primitiveType) {
      case BOOLEAN, BYTE -> (Object record) -> Byte.BYTES;
      case SHORT -> (Object record) -> Short.BYTES;
      case CHARACTER -> (Object record) -> Character.BYTES;
      case INTEGER -> (Object record) -> Integer.BYTES;
      case LONG -> (Object record) -> Long.BYTES;
      case FLOAT -> (Object record) -> Float.BYTES;
      case DOUBLE -> (Object record) -> Double.BYTES;
    };
  }

  static @NotNull ToIntFunction<Object> buildPrimitiveArraySizer(TypeExpr.PrimitiveValueType primitiveType, MethodHandle accessor) {
    final int bytesPerElement = switch (primitiveType) {
      case BOOLEAN, BYTE -> Byte.BYTES;
      case SHORT -> Short.BYTES;
      case CHARACTER -> Character.BYTES;
      case INTEGER -> Integer.BYTES;
      case LONG -> Long.BYTES;
      case FLOAT -> Float.BYTES;
      case DOUBLE -> Double.BYTES;
    };
    return (Object record) -> {
      final Object value;
      try {
        value = accessor.invokeWithArguments(record);
      } catch (Throwable e) {
        throw new RuntimeException(e.getMessage(), e);
      }
      // type maker, length, element * size
      return 2 * Integer.BYTES + Array.getLength(value) * bytesPerElement;
    };
  }

  /// Build a writer for an Enum type
  /// This has to write out the typeOrdinal first as they would be more than one enum type in the system
  /// Then it writes out the typeSignature for the enum class
  /// Finally, it writes out the ordinal of the enum constant
  static @NotNull BiConsumer<ByteBuffer, Object> buildEnumWriter(final Map<Class<?>, TypeInfo> classToTypeInfo, final MethodHandle methodHandle) {
    LOGGER.fine(() -> "Building writer chain for Enum");
    return (ByteBuffer buffer, Object record) -> {
      try {
        // FIXME: we may have many user types that are enums so we need to write out the typeOrdinal and typeSignature
        Enum<?> enumValue = (Enum<?>) methodHandle.invokeWithArguments(record);
        final var typeInfo = classToTypeInfo.get(enumValue.getDeclaringClass());
        ZigZagEncoding.putInt(buffer, typeInfo.typeOrdinal());
        ZigZagEncoding.putLong(buffer, typeInfo.typeSignature());
        int ordinal = enumValue.ordinal();
        ZigZagEncoding.putInt(buffer, ordinal);
      } catch (Throwable e) {
        throw new RuntimeException("Failed to write Enum: " + e.getMessage(), e);
      }
    };
  }

  static @NotNull BiConsumer<ByteBuffer, Object> buildRecordWriter(
      final TypeInfo typeInfo,
      final MethodHandle methodHandle) {
    final int ordinal = typeInfo.typeOrdinal();
    final long typeSignature = typeInfo.typeSignature();
    LOGGER.fine(() -> "Building writer chain for Record with typeOrdinal: " + ordinal + " and typeSignature: 0x" + Long.toHexString(typeSignature));
    final var componentWriters = typeInfo.componentWriters().get();
    return (ByteBuffer buffer, Object object) -> {
      if (object instanceof Record record) {
        // Write the type ordinal and signature first
        ZigZagEncoding.putInt(buffer, ordinal);
        ZigZagEncoding.putLong(buffer, typeSignature);
        // Now write the record components using the pre-built writer chain
        for (int i = 0; i < componentWriters.length; i++) {
          BiConsumer<ByteBuffer, Object> writer = componentWriters[i];
          if (writer != null) {
            writer.accept(buffer, record);
          } else {
            throw new AssertionError("No writer found for component index: " + i + " in record: " + record.getClass().getSimpleName());
          }
        }
      } else
        throw new IllegalArgumentException("expected record type but got: " + object.getClass().getName() + " with method handle: " + methodHandle);
    };
  }

  static @NotNull BiConsumer<ByteBuffer, Object> buildValueWriter(final TypeExpr.RefValueType refValueType, final MethodHandle methodHandle) {
    return switch (refValueType) {
      case BOOLEAN -> {
        LOGGER.fine(() -> "Building writer chain for boolean.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            final var result = methodHandle.invokeWithArguments(record);
            buffer.put((byte) ((boolean) result ? 1 : 0));
          } catch (Throwable e) {
            throw new RuntimeException("Failed to write boolean value", e);
          }
        };
      }
      case BYTE -> {
        LOGGER.fine(() -> "Building writer chain for byte.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            buffer.put((byte) methodHandle.invokeWithArguments(record));
          } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
          }
        };
      }
      case UUID -> {
        LOGGER.fine(() -> "Building writer chain for UUID");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            UUID uuid = (UUID) methodHandle.invokeWithArguments(record);
            buffer.putLong(uuid.getMostSignificantBits());
            buffer.putLong(uuid.getLeastSignificantBits());
          } catch (Throwable e) {
            throw new RuntimeException("Failed to write UUID: " + e.getMessage(), e);
          }
        };
      }
      case SHORT -> {
        LOGGER.fine(() -> "Building writer chain for short.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            buffer.putShort((short) methodHandle.invokeWithArguments(record));
          } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
          }
        };
      }
      case CHARACTER -> {
        LOGGER.fine(() -> "Building writer chain for char.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            buffer.putChar((char) methodHandle.invokeWithArguments(record));
          } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
          }
        };
      }
      case INTEGER -> {
        LOGGER.fine(() -> "Building writer chain for int.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            Object result = methodHandle.invokeWithArguments(record);
            buffer.putInt((int) result);
          } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
          }
        };
      }
      case LONG -> {
        LOGGER.fine(() -> "Building writer chain for long.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            buffer.putLong((long) methodHandle.invokeWithArguments(record));
          } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
          }
        };
      }
      case FLOAT -> {
        LOGGER.fine(() -> "Building writer chain for float.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            buffer.putFloat((float) methodHandle.invokeWithArguments(record));
          } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
          }
        };
      }
      case DOUBLE -> {
        LOGGER.fine(() -> "Building writer chain for double.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            buffer.putDouble((double) methodHandle.invokeWithArguments(record));
          } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
          }
        };
      }
      case STRING -> {
        LOGGER.fine(() -> "Building writer chain for String");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            String str = (String) methodHandle.invokeWithArguments(record);
            byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
            ZigZagEncoding.putInt(buffer, bytes.length);
            buffer.put(bytes);
          } catch (Throwable e) {
            throw new RuntimeException("Failed to write String: " + e.getMessage(), e);
          }
        };
      }
      default -> throw new AssertionError("not implemented yet ref value type: " + refValueType);
    };
  }

  static @NotNull Function<ByteBuffer, Object> buildEnumReader(final Map<Class<?>, TypeInfo> classToTypeInfo) {
    LOGGER.fine(() -> "Building reader chain for Enum");
    return (ByteBuffer buffer) -> {
      try {
        LOGGER.fine(() -> "Reading Enum from buffer at position: " + buffer.position());
        // FIXME: we may have many user types that are enums so we need to write out the typeOrdinal and typeSignature
        int typeOrdinal = ZigZagEncoding.getInt(buffer);
        long typeSignature = ZigZagEncoding.getLong(buffer);
        Class<?> enumClass = classToTypeInfo.entrySet().stream()
            .filter(e -> e.getValue().typeOrdinal() == typeOrdinal
                && e.getValue().typeSignature() == typeSignature).map(Map.Entry::getKey)
            .findFirst().orElseThrow(() -> new IllegalArgumentException("Unknown enum type ordinal: " + typeOrdinal + " with signature: " + Long.toHexString(typeSignature)));

        int ordinal = ZigZagEncoding.getInt(buffer);
        return enumClass.getEnumConstants()[ordinal];
      } catch (Throwable e) {
        throw new RuntimeException("Failed to read Enum: " + e.getMessage(), e);
      }
    };
  }

  static @NotNull Function<ByteBuffer, Object> buildRecordReader(TypeInfo typeInfo) {
    LOGGER.fine(() -> "Building reader chain for Record with typeOrdinal " + typeInfo.typeOrdinal());
    return (ByteBuffer buffer) -> {
      throw new AssertionError("not implemented: Record reader for record with classToTypeInfo: " + typeInfo);
    };
  }

  static @NotNull Function<ByteBuffer, Object> buildValueReader(TypeExpr.RefValueType valueType) {
    LOGGER.fine(() -> "Building reader chain for RefValueType: " + valueType);
    return switch (valueType) {
      case BOOLEAN -> (buffer) -> buffer.get() != 0;
      case BYTE -> ByteBuffer::get;
      case SHORT -> ByteBuffer::getShort;
      case CHARACTER -> ByteBuffer::getChar;
      case INTEGER -> ByteBuffer::getInt;
      case LONG -> ByteBuffer::getLong;
      case FLOAT -> ByteBuffer::getFloat;
      case DOUBLE -> ByteBuffer::getDouble;
      case UUID -> (buffer) -> {
        long most = buffer.getLong();
        long least = buffer.getLong();
        return new UUID(most, least);
      };
      case STRING -> (buffer) -> {
        int length = ZigZagEncoding.getInt(buffer);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
      };
      default -> throw new AssertionError("not implemented yet ref value type: " + valueType);
    };
  }

  static @NotNull ToIntFunction<Object> buildValueSizer(TypeExpr.RefValueType refValueType, MethodHandle accessor) {
    return switch (refValueType) {
      case BOOLEAN, BYTE -> (Object record) -> Byte.BYTES;
      case SHORT -> (Object record) -> Short.BYTES;
      case CHARACTER -> (Object record) -> Character.BYTES;
      case INTEGER -> (Object record) -> Integer.BYTES;
      case LONG -> (Object record) -> Long.BYTES;
      case FLOAT -> (Object record) -> Float.BYTES;
      case DOUBLE -> (Object record) -> Double.BYTES;
      case UUID -> (Object record) -> 2 * Long.BYTES; // UUID is two longs
      case ENUM -> (Object record) -> Integer.BYTES + 2 * Long.BYTES; // typeOrdinal + typeSignature + enum ordinal
      case STRING -> (Object record) -> {
        try {
          // Worst case estimate users the length then one int per UTF-8 encoded character
          String str = (String) accessor.invokeWithArguments(record);
          return (str.length() + 1) * Integer.BYTES;
        } catch (Throwable e) {
          throw new RuntimeException("Failed to size String", e);
        }
      };
      case RECORD -> (Object obj) -> {
        Record record = (Record) obj;
        //return 1 + Integer.BYTES + CLASS_SIG_BYTES + maxSizeOfRecordComponents(record);
        throw new AssertionError("not implemented: meed to lookup ad deelgat to Record sizer for record: " + record.getClass().getSimpleName());
      };
      case INTERFACE -> (Object userType) -> {
        if (userType instanceof Enum<?> ignored) {
          // For enums, we store the ordinal and type signature
          return Integer.BYTES + Long.BYTES;
        } else if (userType instanceof Record record) {
          //return 1 + Integer.BYTES + CLASS_SIG_BYTES + maxSizeOfRecordComponents(record);
          throw new AssertionError("not implemented: meed to lookup and delegate to Record sizer for record: " + record.getClass().getSimpleName());
        } else {
          throw new IllegalArgumentException("Unsupported interface type: " + userType.getClass().getSimpleName());
        }
      };
    };
  }

  int maxSizeOfRecordComponents(Record record) {
    int totalSize = 0;
    for (ToIntFunction<Object> sizer : this.componentSizers) {
      totalSize += sizer.applyAsInt(record);
    }
    return totalSize;
  }

  static @NotNull ToIntFunction<Object> buildRecordSizer(TypeInfo typeInfo, TypeExpr.RefValueType refValueType, MethodHandle accessor) {
    throw new AssertionError("not implemented: Record sizer for ref value type: " + refValueType + " with accessor: " + accessor);
  }

  @Override
  public int serialize(ByteBuffer buffer, T record) {
    Objects.requireNonNull(buffer, "buffer cannot be null");
    Objects.requireNonNull(record, "record cannot be null");
    if (!this.userType.isAssignableFrom(record.getClass())) {
      throw new IllegalArgumentException("Expected a record type " + this.userType.getName() +
          " but got: " + record.getClass().getName());
    }
    buffer.order(ByteOrder.BIG_ENDIAN);
    final var startPosition = buffer.position();

    writeToWire(buffer, record);

    final var totalBytes = buffer.position() - startPosition;
    LOGGER.finer(() -> "PicklerImpl.serialize: completed, totalBytes=" + totalBytes);
    return totalBytes;
  }

  void writeToWire(ByteBuffer buffer, T record) {
    final var startPosition = buffer.position();
    LOGGER.finer(() -> "Serializing record  " + record.getClass().getSimpleName() +
        " hashCode " + record.hashCode() +
        " position " + startPosition +
        " typeSignature 0x" + Long.toHexString(typeSignature) +
        " buffer remaining bytes: " + buffer.remaining() + " limit: " +
        buffer.limit() + " capacity: " + buffer.capacity()
    );
    // write the signature first as it is a cryptographic hash of the class name and component metadata and fixed size
    buffer.putLong(typeSignature);
    serializeRecordComponents(buffer, record);
  }

  void serializeRecordComponents(ByteBuffer buffer, T record) {
    LOGGER.finer(() -> "Serializing " + componentWriters.length + " components for " + record.getClass().getSimpleName() +
        " at position " + buffer.position());
    IntStream.range(0, componentWriters.length).forEach(i -> {
      final int componentIndex = i; // final for lambda capture
      LOGGER.finer(() -> "Writing component " + componentIndex + " at position " + buffer.position());
      componentWriters[componentIndex].accept(buffer, record);
      LOGGER.finer(() -> "Finished component " + componentIndex + " at position " + buffer.position());
    });
  }

  @Override
  public T deserialize(ByteBuffer buffer) {
    Objects.requireNonNull(buffer);
    buffer.order(ByteOrder.BIG_ENDIAN);
    return readFromWire(buffer);
  }

  T readFromWire(ByteBuffer buffer) {
    final int typeSigPosition = buffer.position();
    // read the type signature first as it is a cryptographic hash of the class name and component metadata and fixed size
    final long signature = buffer.getLong();
    LOGGER.finer(() -> "Deserializing record " + this.userType.getSimpleName() + " position " +
        typeSigPosition + " signature 0x" + Long.toHexString(signature) +
        " buffer remaining bytes: " + buffer.remaining() + " limit: " +
        buffer.limit() + " capacity: " + buffer.capacity());
    if (signature != this.typeSignature) {
      throw new IllegalStateException("Expected type signature 0x" + Long.toHexString(this.typeSignature) +
          " but got: 0x" + Long.toHexString(signature) + " at position: " + typeSigPosition);
    }
    Object[] components = new Object[componentReaders.length];
    IntStream.range(0, componentReaders.length).forEach(i -> {
      final int componentIndex = i; // final for lambda capture
      final int beforePosition = buffer.position();
      LOGGER.finer(() -> "Reading component " + componentIndex + " at position " + beforePosition);
      components[i] = componentReaders[i].apply(buffer);
      final Object componentValue = components[i]; // final for lambda capture
      final int afterPosition = buffer.position();
      LOGGER.finer(() -> "Read component " + componentIndex + ": " + componentValue + " moved from position " + beforePosition + " to " + afterPosition);
    });

    // Invoke constructor
    try {
      LOGGER.finer(() -> "Constructing record at position " + buffer.position() + " with components: " + Arrays.toString(components));
      //noinspection unchecked we know by static inspection that this is safe
      return (T) this.recordConstructor.invokeWithArguments(components);
    } catch (Throwable e) {
      throw new RuntimeException("Failed to construct record", e);
    }
  }

  @Override
  public int maxSizeOf(T record) {
    Objects.requireNonNull(record);
    if (!this.userType.isAssignableFrom(record.getClass())) {
      throw new IllegalArgumentException("Expected a record type " + this.userType.getName() +
          " but got: " + record.getClass().getName());
    }
    int size = CLASS_SIG_BYTES + Integer.BYTES; // signature bytes then ordinal marker
    size += maxSizeOfRecordComponents((Record) record);
    return size;
  }
}
