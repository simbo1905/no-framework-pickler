// SPDX-FileCopyrightText: 2025 Simon Massey  
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
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

import static io.github.simbo1905.no.framework.Constants.INTEGER;
import static io.github.simbo1905.no.framework.Constants.INTEGER_VAR;
import static io.github.simbo1905.no.framework.Pickler.LOGGER;

sealed interface Companion permits Companion.Nothing {

  record Nothing() implements Companion {
  }

  String SHA_256 = "SHA-256";
  int SAMPLE_SIZE = 32;
  int CLASS_SIG_BYTES = Long.BYTES;
  byte NULL_MARKER = (byte) -1;
  byte NOT_NULL_MARKER = (byte) 1;

  /// Discover all reachable types from a root class including sealed hierarchies and record components
  static Set<Class<?>> recordClassHierarchy(final Class<?> current) {
    return recordClassHierarchyInner(current, new HashSet<>()).collect(Collectors.toSet());
  }

  /// Discover all reachable types from a root class including sealed hierarchies and record components
  static Stream<Class<?>> recordClassHierarchyInner(final Class<?> current, final Set<Class<?>> visited) {
    if (!visited.add(current)) {
      return Stream.empty();
    }

    // Handle array types - discover their component types
    if (current.isArray()) {
      Class<?> componentType = current.getComponentType();
      LOGGER.finer(() -> "Root is array type: " + current.getSimpleName() + " with component type: " + componentType.getSimpleName());
      // Include both the array type and its component type
      return Stream.concat(
          Stream.of(current),
          recordClassHierarchyInner(componentType, visited)
      );
    }

    return Stream.concat(
        Stream.of(current),
        Stream.concat(
            current.isSealed()
                ? Arrays.stream(current.getPermittedSubclasses())
                : Stream.empty(),

            current.isRecord()
                ? Arrays.stream(current.getRecordComponents())
                .flatMap(component -> {
                  LOGGER.finer(() -> "Analyzing component " + component.getName() + " with type " + component.getGenericType());
                  Class<?> directType = component.getType();

                  // Search array to check if they contain user-defined types
                  Stream<Class<?>> arrayStream = Stream.empty();
                  if (directType.isArray()) {
                    Class<?> componentType = directType.getComponentType();
                    if (componentType.isRecord() || componentType.isEnum() || !componentType.isPrimitive()) {
                      LOGGER.finer(() -> "Including array type: " + directType.getSimpleName());
                      arrayStream = Stream.of(directType);
                    }
                  }

                  TypeExpr structure = TypeExpr.analyzeType(component.getGenericType());
                  LOGGER.finer(() -> "Component " + component.getName() + " discovered types: " +
                      structure.toTreeString());
                  Stream<Class<?>> structureStream = TypeExpr.classesInAST(structure);
                  return Stream.concat(arrayStream, structureStream);
                })
                : Stream.empty()
        ).flatMap(child -> recordClassHierarchyInner(child, visited))
    );
  }


  static Class<?> extractComponentType(TypeExpr typeExpr) {
    // Simplified component type extraction
    return switch (typeExpr) {
      case TypeExpr.RefValueNode(var ignored, var javaType) -> (Class<?>) javaType;
      case TypeExpr.OptionalNode(var ignored) -> Optional.class;
      case TypeExpr.ListNode(var ignored) -> List.class;
      case TypeExpr.ArrayNode(var ignored) -> Object[].class;
      case TypeExpr.MapNode(var ignoredKey, var ignoredValue) -> Map.class;
      default -> Object.class;
    };
  }

  public static @NotNull BiConsumer<ByteBuffer, Object> buildPrimitiveArrayWriterInner(TypeExpr.PrimitiveValueType primitiveType) {
    LOGGER.fine(() -> "Building writer chain for primitive array type: " + primitiveType);
    return switch (primitiveType) {
      case BOOLEAN -> (buffer, inner) -> {
        final var position = buffer.position();
        final var booleans = (boolean[]) inner;
        ZigZagEncoding.putInt(buffer, Constants.BOOLEAN.marker());
        int length = booleans.length;
        ZigZagEncoding.putInt(buffer, length);
        BitSet bitSet = new BitSet(length);
        // Create a BitSet and flip bits to try where necessary
        IntStream.range(0, length).filter(i -> booleans[i]).forEach(bitSet::set);
        byte[] bytes = bitSet.toByteArray();
        ZigZagEncoding.putInt(buffer, bytes.length);
        buffer.put(bytes);
        LOGGER.finer(() -> "Written primitive ARRAY for tag BOOLEAN at position " + position + " with length=" + length + " and bytes length=" + bytes.length);
      };
      case BYTE -> (buffer, inner) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag BYTE at position " + buffer.position());
        final var bytes = (byte[]) inner;
        ZigZagEncoding.putInt(buffer, Constants.BYTE.marker());
        ZigZagEncoding.putInt(buffer, bytes.length);
        buffer.put(bytes);
      };
      case SHORT -> (buffer, inner) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag SHORT at position " + buffer.position());
        ZigZagEncoding.putInt(buffer, Constants.SHORT.marker());
        final var shorts = (short[]) inner;
        ZigZagEncoding.putInt(buffer, shorts.length);
        for (short s : shorts) {
          buffer.putShort(s);
        }
      };
      case CHARACTER -> (buffer, inner) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag CHARACTER at position " + buffer.position());
        final var chars = (char[]) inner;
        ZigZagEncoding.putInt(buffer, Constants.CHARACTER.marker());
        ZigZagEncoding.putInt(buffer, chars.length);
        for (char c : chars) {
          buffer.putChar(c);
        }
      };
      case FLOAT -> (buffer, inner) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag FLOAT at position " + buffer.position());
        final var floats = (float[]) inner;
        ZigZagEncoding.putInt(buffer, Constants.FLOAT.marker());
        ZigZagEncoding.putInt(buffer, floats.length);
        for (float f : floats) {
          buffer.putFloat(f);
        }
      };
      case DOUBLE -> (buffer, inner) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag DOUBLE at position " + buffer.position());
        ZigZagEncoding.putInt(buffer, Constants.DOUBLE.marker());
        final var doubles = (double[]) inner;
        ZigZagEncoding.putInt(buffer, doubles.length);
        for (double d : doubles) {
          buffer.putDouble(d);
        }
      };
      case INTEGER -> (buffer, inner) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag INTEGER at position " + buffer.position());
        final var integers = (int[]) inner;
        final var length = Array.getLength(inner);
        final var sampleAverageSize = length > 0 ? estimateAverageSizeInt(integers, length) : 1;
        // Here we must be saving one byte per integer to justify the encoding cost
        if (sampleAverageSize < Integer.BYTES - 1) {
          LOGGER.finer(() -> "Delegating ARRAY for tag " + INTEGER_VAR + " with length=" + Array.getLength(inner) + " at position " + buffer.position());
          ZigZagEncoding.putInt(buffer, INTEGER_VAR.marker());
          ZigZagEncoding.putInt(buffer, length);
          for (int i : integers) {
            ZigZagEncoding.putInt(buffer, i);
          }
        } else {
          LOGGER.finer(() -> "Delegating ARRAY for tag " + INTEGER + " with length=" + Array.getLength(inner) + " at position " + buffer.position());
          ZigZagEncoding.putInt(buffer, INTEGER.marker());
          ZigZagEncoding.putInt(buffer, length);
          for (int i : integers) {
            buffer.putInt(i);
          }
        }
      };
      case LONG -> (buffer, inner) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag LONG at position " + buffer.position());
        final var longs = (long[]) inner;
        final var length = Array.getLength(inner);
        final var sampleAverageSize = length > 0 ? estimateAverageSizeLong(longs, length) : 1;
        if ((length <= SAMPLE_SIZE && sampleAverageSize < Long.BYTES - 1) || (length > SAMPLE_SIZE && sampleAverageSize < Long.BYTES - 2)) {
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

  static int estimateAverageSizeLong(long[] longs, int length) {
    final var sampleLength = Math.min(length, SAMPLE_SIZE);
    final var sampleSize = IntStream.range(0, sampleLength)
        .map(i -> ZigZagEncoding.sizeOf(longs[i]))
        .sum();
    return sampleSize / sampleLength;
  }

  static int estimateAverageSizeInt(int[] integers, int length) {
    final var sampleLength = Math.min(length, SAMPLE_SIZE);
    final var sampleSize = IntStream.range(0, sampleLength)
        .map(i -> ZigZagEncoding.sizeOf(integers[i]))
        .sum();
    return sampleSize / sampleLength;
  }


  static @NotNull BiConsumer<ByteBuffer, Object> buildPrimitiveValueWriterInner(TypeExpr.PrimitiveValueType primitiveType) {
    return switch (primitiveType) {
      case BOOLEAN -> (ByteBuffer buffer, Object result) -> buffer.put((byte) ((boolean) result ? 1 : 0));
      case BYTE -> (ByteBuffer buffer, Object result) -> buffer.put((byte) result);
      case SHORT -> (ByteBuffer buffer, Object result) -> buffer.putShort((short) result);
      case CHARACTER -> (ByteBuffer buffer, Object result) -> buffer.putChar((char) result);
      case INTEGER -> (ByteBuffer buffer, Object object) -> {
        final int result = (int) object;
        final var position = buffer.position();
        if (ZigZagEncoding.sizeOf(result) < Integer.BYTES) {
          ZigZagEncoding.putInt(buffer, INTEGER_VAR.marker());
          LOGGER.fine(() -> "Writing INTEGER_VAR value=" + result + " at position: " + position);
          ZigZagEncoding.putInt(buffer, result);
        } else {
          ZigZagEncoding.putInt(buffer, INTEGER.marker());
          LOGGER.fine(() -> "Writing INTEGER value=" + result + " at position: " + position);
          buffer.putInt(result);
        }
      };
      case LONG -> {
        LOGGER.fine(() -> "Building writer chain for long.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          final long result = (long) record;
          final var position = buffer.position();
          if (ZigZagEncoding.sizeOf(result) < Long.BYTES) {
            ZigZagEncoding.putInt(buffer, Constants.LONG_VAR.marker());
            LOGGER.fine(() -> "Writing LONG_VAR value=" + result + " at position: " + position);
            ZigZagEncoding.putLong(buffer, result);
          } else {
            ZigZagEncoding.putInt(buffer, Constants.LONG.marker());
            LOGGER.finer(() -> "Writing LONG value=" + result + " at position: " + position);
            buffer.putLong(result);
          }
        };
      }
      case FLOAT -> (ByteBuffer buffer, Object inner) -> buffer.putFloat((Float) inner);
      case DOUBLE -> (ByteBuffer buffer, Object inner) -> buffer.putDouble((Double) inner);
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
        final int marker = ZigZagEncoding.getInt(buffer);
        if (marker == INTEGER_VAR.marker()) {
          int value = ZigZagEncoding.getInt(buffer);
          LOGGER.finer(() -> "INTEGER reader: read INTEGER_VAR value=" + value + " at position=" + buffer.position());
          return value;
        } else if (marker == INTEGER.marker()) {
          int value = buffer.getInt();
          LOGGER.finer(() -> "INTEGER reader: read INTEGER value=" + value + " at position=" + buffer.position());
          return value;
        } else
          throw new IllegalStateException("Expected INTEGER or INTEGER_VAR marker but got: " + marker + " at position: " + position);
      };
      case LONG -> (buffer) -> {
        final var position = buffer.position();
        // If not enough space, read marker
        final int marker = ZigZagEncoding.getInt(buffer);
        if (marker == Constants.LONG_VAR.marker()) {
          LOGGER.finer(() -> "LONG reader: read LONG_VAR marker=" + marker + " at position=" + position);
          return ZigZagEncoding.getLong(buffer);
        } else if (marker == Constants.LONG.marker()) {
          LOGGER.finer(() -> "LONG reader: read LONG marker=" + marker + " at position=" + position);
          return buffer.getLong();
        } else {
          throw new IllegalStateException("Expected LONG or LONG_VAR marker but got: " + marker + " at position: " + position);
        }
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
        if (marker == INTEGER_VAR.marker()) {
          int length = ZigZagEncoding.getInt(buffer);
          int[] integers = new int[length];
          IntStream.range(0, length).forEach(i -> integers[i] = ZigZagEncoding.getInt(buffer));
          return integers;
        } else if (marker == INTEGER.marker()) {
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

  static @NotNull ToIntFunction<Object> buildPrimitiveValueSizer(TypeExpr.PrimitiveValueType primitiveType) {
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

  static @NotNull ToIntFunction<Object> buildPrimitiveArraySizerInner(TypeExpr.PrimitiveValueType primitiveType) {
    final int bytesPerElement = switch (primitiveType) {
      case BOOLEAN, BYTE -> Byte.BYTES;
      case SHORT -> Short.BYTES;
      case CHARACTER -> Character.BYTES;
      case INTEGER -> Integer.BYTES;
      case LONG -> Long.BYTES;
      case FLOAT -> Float.BYTES;
      case DOUBLE -> Double.BYTES;
    };
    return (Object value) -> 2 * Integer.BYTES + Array.getLength(value) * bytesPerElement;
  }

  static <X> void writeToWireWitness(RecordPickler<X> rp, ByteBuffer buffer, Object record) {
    LOGGER.fine(() -> "Writing record to wire using " + rp + " for record: " + record.getClass().getSimpleName() + " at position: " + buffer.position());
    //noinspection unchecked
    rp.writeToWire(buffer, (X) record);
  }

  /// Compute a CLASS_SIG_BYTES signature from enum class and constant names
  static long hashEnumSignature(Class<?> enumClass) {
    try {
      MessageDigest digest = MessageDigest.getInstance(SHA_256);

      Object[] enumConstants = enumClass.getEnumConstants();
      assert enumConstants != null : "Not an enum class: " + enumClass;

      String input = Stream.concat(Stream.of(enumClass.getSimpleName()), Arrays.stream(enumConstants).map(e -> ((Enum<?>) e).name())).collect(Collectors.joining("!"));

      byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));

      return IntStream.range(0, CLASS_SIG_BYTES).mapToLong(i -> (hash[i] & 0xFFL) << (56 - i * 8)).reduce(0L, (a, b) -> a | b);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(SHA_256 + " not available", e);
    }
  }

  static final Set<Class<?>> BOXED_PRIMITIVES = Set.of(
      Byte.class, Short.class, Integer.class, Long.class,
      Float.class, Double.class, Character.class, Boolean.class
  );

  static Class<?> typeExprToClass(TypeExpr typeExpr) {
    return switch (typeExpr) {
      case TypeExpr.ArrayNode(var element) -> {
        Class<?> componentClass = typeExprToClass(element);
        yield Array.newInstance(componentClass, 0).getClass();
      }
      case TypeExpr.RefValueNode(var ignored, var javaType) -> (Class<?>) javaType;
      case TypeExpr.PrimitiveValueNode(var ignored, var javaType) -> (Class<?>) javaType;
      case TypeExpr.ListNode ignored -> List.class;
      case TypeExpr.MapNode ignored -> Map.class;
      case TypeExpr.OptionalNode ignored -> Optional.class;
    };
  }

  static long hashSignature(String uniqueNess) throws NoSuchAlgorithmException {
    long result;
    MessageDigest digest = MessageDigest.getInstance(SHA_256);

    byte[] hash = digest.digest(uniqueNess.getBytes(StandardCharsets.UTF_8));

    // Convert first CLASS_SIG_BYTES to long
    //      Byte Index:   0       1       2        3        4        5        6        7
    //      Bits:      [56-63] [48-55] [40-47] [32-39] [24-31] [16-23] [ 8-15] [ 0-7]
    //      Shift:      <<56   <<48   <<40    <<32    <<24    <<16    <<8     <<0
    result = IntStream.range(0, CLASS_SIG_BYTES).mapToLong(i -> (hash[i] & 0xFFL) << (56 - i * 8)).reduce(0L, (a, b) -> a | b);
    return result;
  }

  /// Compute a CLASS_SIG_BYTES signature from class name and component metadata
  static long hashClassSignature(Class<?> clazz, RecordComponent[] components, TypeExpr[] componentTypes) {
    String input = Stream.concat(Stream.of(clazz.getSimpleName()), IntStream.range(0, components.length).boxed().flatMap(i -> Stream.concat(Stream.of(componentTypes[i].toTreeString()), Stream.of(components[i].getName())))).collect(Collectors.joining("!"));
    try {
      return hashSignature(input);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }
}
