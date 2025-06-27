// SPDX-FileCopyrightText: 2025 Simon Massey  
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.jetbrains.annotations.NotNull;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Array;
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

import static io.github.simbo1905.no.framework.Constants.INTEGER_VAR;
import static io.github.simbo1905.no.framework.PicklerImpl.*;
import static io.github.simbo1905.no.framework.Tag.INTEGER;

class Companion {
  /// Discover all reachable types from a root class including sealed hierarchies and record components
  static Stream<Class<?>> recordClassHierarchy(final Class<?> current, final Set<Class<?>> visited) {
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
          recordClassHierarchy(componentType, visited)
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

                  TypeStructure structure = TypeStructure.analyze(component.getGenericType());
                  LOGGER.finer(() -> "Component " + component.getName() + " discovered types: " +
                      structure.tagTypes().stream().map(TagWithType::type).map(Class::getSimpleName).toList());
                  Stream<Class<?>> structureStream = structure.tagTypes().stream().map(TagWithType::type);
                  return Stream.concat(arrayStream, structureStream);
                })
                .filter(t -> t.isRecord() || t.isSealed() || t.isEnum() ||
                    (t.isArray() && (t.getComponentType().isRecord() || t.getComponentType().isEnum() || !t.getComponentType().isPrimitive())))
                : Stream.empty()
        ).flatMap(child -> recordClassHierarchy(child, visited))
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

  static BiConsumer<ByteBuffer, Object> buildReferenceArrayWriterInner(TypeExpr.RefValueType refType) {
    return (buffer, object) -> {
      Object[] array = (Object[]) object;
      ZigZagEncoding.putInt(buffer, Constants.ARRAY_REF.marker());
      ZigZagEncoding.putInt(buffer, array.length);

      // Write each element based on reference type
      for (Object item : array) {
        switch (refType) {
          case STRING -> {
            String str = (String) item;
            byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
            ZigZagEncoding.putInt(buffer, bytes.length);
            buffer.put(bytes);
          }
          case INTEGER -> buffer.putInt((Integer) item);
          case BOOLEAN -> buffer.put((byte) ((Boolean) item ? 1 : 0));
          // Add other reference types as needed
          default -> throw new UnsupportedOperationException("Unsupported ref type: " + refType);
        }
      }
    };
  }

  static Function<ByteBuffer, Object> buildReferenceArrayReader(TypeExpr.RefValueType refType) {
    return buffer -> {
      int marker = ZigZagEncoding.getInt(buffer);
      assert marker == Constants.ARRAY_REF.marker() : "Expected ARRAY_REF marker";
      int length = ZigZagEncoding.getInt(buffer);

      return switch (refType) {
        case STRING -> {
          String[] array = new String[length];
          for (int i = 0; i < length; i++) {
            int strLength = ZigZagEncoding.getInt(buffer);
            byte[] bytes = new byte[strLength];
            buffer.get(bytes);
            array[i] = new String(bytes, StandardCharsets.UTF_8);
          }
          yield array;
        }
        case INTEGER -> {
          Integer[] array = new Integer[length];
          for (int i = 0; i < length; i++) {
            array[i] = buffer.getInt();
          }
          yield array;
        }
        case BOOLEAN -> {
          Boolean[] array = new Boolean[length];
          for (int i = 0; i < length; i++) {
            array[i] = buffer.get() != 0;
          }
          yield array;
        }
        default -> throw new UnsupportedOperationException("Unsupported ref type: " + refType);
      };
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
          ZigZagEncoding.putInt(buffer, Constants.INTEGER.marker());
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
        if ((length <= RecordPickler.SAMPLE_SIZE && sampleAverageSize < Long.BYTES - 1) || (length > RecordPickler.SAMPLE_SIZE && sampleAverageSize < Long.BYTES - 2)) {
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
          ZigZagEncoding.putInt(buffer, Constants.INTEGER.marker());
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
        } else if (marker == Constants.INTEGER.marker()) {
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

  static @NotNull BiConsumer<ByteBuffer, Object> buildValueWriterInner(final TypeExpr.RefValueType refValueType) {

    return switch (refValueType) {
      case BOOLEAN -> {
        LOGGER.fine(() -> "Building writer chain for boolean.class primitive type");
        yield (ByteBuffer buffer, Object result) -> {
          buffer.put((byte) ((boolean) result ? 1 : 0));
        };
      }
      case BYTE -> {
        LOGGER.fine(() -> "Building writer chain for byte.class primitive type");
        yield (ByteBuffer buffer, Object result) -> {
          buffer.put((byte) result);
        };
      }
      case UUID -> {
        LOGGER.fine(() -> "Building writer chain for UUID");
        yield (ByteBuffer buffer, Object inner) -> {
          UUID uuid = (UUID) inner;
          buffer.putLong(uuid.getMostSignificantBits());
          buffer.putLong(uuid.getLeastSignificantBits());
        };
      }
      case SHORT -> {
        LOGGER.fine(() -> "Building writer chain for short.class primitive type");
        yield (ByteBuffer buffer, Object inner) -> buffer.putShort((short) inner);
      }
      case CHARACTER -> {
        LOGGER.fine(() -> "Building writer chain for char.class primitive type");
        yield (ByteBuffer buffer, Object inner) -> buffer.putChar((char) inner);
      }
      case INTEGER -> {
        LOGGER.fine(() -> "Building writer chain for int.class primitive type");
        yield (ByteBuffer buffer, Object result) -> buffer.putInt((int) result);
      }
      case LONG -> {
        LOGGER.fine(() -> "Building writer chain for long.class primitive type");
        yield (ByteBuffer buffer, Object result) -> buffer.putLong((long) result);
      }
      case FLOAT -> {
        LOGGER.fine(() -> "Building writer chain for float.class primitive type");
        yield (ByteBuffer buffer, Object result) -> buffer.putFloat((float) result);
      }
      case DOUBLE -> {
        LOGGER.fine(() -> "Building writer chain for double.class primitive type");
        yield (ByteBuffer buffer, Object result) -> buffer.putDouble((double) result);
      }
      case STRING -> {
        LOGGER.fine(() -> "Building writer chain for String");
        yield (ByteBuffer buffer, Object result) -> {
          String str = (String) result;
          byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
          ZigZagEncoding.putInt(buffer, bytes.length);
          buffer.put(bytes);
        };
      }
      case RECORD -> {
        LOGGER.fine(() -> "Building writer chain for Record");
        yield (ByteBuffer buffer, Object record) -> {
          final var p = PicklerRoot.REGISTRY.computeIfAbsent(record.getClass(), RecordPickler::new);
          switch (p) {
            case EmptyRecordPickler<?> erp -> buffer.putLong(erp.typeSignature);
            case RecordPickler<?> rp -> writeToWireWitness(rp, buffer, record);
            default -> throw new AssertionError("Unexpected pickler type: " + p.getClass());
          }
        };
      }
      default -> throw new AssertionError("not implemented yet ref value type: " + refValueType);
    };
  }

  static <X> void writeToWireWitness(RecordPickler<X> rp, ByteBuffer buffer, Object record) {
    //noinspection unchecked
    rp.writeToWire(buffer, (X) record);
  }

  static @NotNull Function<ByteBuffer, Object> buildEnumReader(final Map<Class<?>, TypeInfo> classToTypeInfo) {
    LOGGER.fine(() -> "Building reader chain for Enum");
    return (ByteBuffer buffer) -> {
      try {
        LOGGER.fine(() -> "Reading Enum from buffer at position: " + buffer.position());
        // FIXME: we may have many user types that are enums so we need to write out the typeOrdinal and typeSignature
        int typeOrdinal = ZigZagEncoding.getInt(buffer);
        long typeSignature = ZigZagEncoding.getLong(buffer);
        Class<?> enumClass = classToTypeInfo.entrySet().stream().filter(e -> e.getValue().typeOrdinal() == typeOrdinal && e.getValue().typeSignature() == typeSignature).map(Map.Entry::getKey).findFirst().orElseThrow(() -> new IllegalArgumentException("Unknown enum type ordinal: " + typeOrdinal + " with signature: " + Long.toHexString(typeSignature)));

        int ordinal = ZigZagEncoding.getInt(buffer);
        return enumClass.getEnumConstants()[ordinal];
      } catch (Throwable e) {
        throw new RuntimeException("Failed to read Enum: " + e.getMessage(), e);
      }
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

  static @NotNull ToIntFunction<Object> buildValueSizerInner(TypeExpr.RefValueType refValueType) {
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
      case STRING -> (Object inner) -> {
        // Worst case estimate users the length then one int per UTF-8 encoded character
        String str = (String) inner;
        return (str.length() + 1) * Integer.BYTES;
      };
      case RECORD -> (Object obj) -> {
        Record record = (Record) obj;
        //return 1 + Integer.BYTES + CLASS_SIG_BYTES + maxSizeOfRecordComponents(record);
        // FIXME: why have i not hit this yet?
        throw new AssertionError("not implemented: meed to lookup and delegate to Record sizer for record: " + record.getClass().getSimpleName());
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

  /// Compute a CLASS_SIG_BYTES signature from enum class and constant names
  static long hashEnumSignature(Class<?> enumClass) {
    try {
      MessageDigest digest = MessageDigest.getInstance(SHA_256);

      Object[] enumConstants = enumClass.getEnumConstants();
      assert enumConstants != null : "Not an enum class: " + enumClass;

      String input = Stream.concat(Stream.of(enumClass.getSimpleName()), Arrays.stream(enumConstants).map(e -> ((Enum<?>) e).name())).collect(Collectors.joining("!"));

      byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));

      return IntStream.range(0, RecordPickler.CLASS_SIG_BYTES).mapToLong(i -> (hash[i] & 0xFFL) << (56 - i * 8)).reduce(0L, (a, b) -> a | b);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(SHA_256 + " not available", e);
    }
  }

  /// Create writer for linear container types like Optional, List, or Array (but not map)
  static BiConsumer<ByteBuffer, Object> createContainerWriter(TypeExpr typeExpr, BiConsumer<ByteBuffer, Object> elementWriter) {
    if (typeExpr.isContainer()) {
      if (typeExpr instanceof TypeExpr.ListNode(TypeExpr element)) {
        LOGGER.fine(() -> "Creating writer for ListNode with element type: " + element.toTreeString());
        return (buffer, value) -> {
          List<?> list = (List<?>) value;
          LOGGER.fine(() -> "Writing LIST size=" + list.size());
          ZigZagEncoding.putInt(buffer, Constants.LIST.marker());
          buffer.putInt(list.size());
          for (Object item : list) {
            elementWriter.accept(buffer, item);
          }
        };
      }
    }
    throw new IllegalArgumentException("Unsupported container type: " + typeExpr.toTreeString());
  }

  static BiConsumer<ByteBuffer, Object> buildObjectArrayWriter(TypeExpr elementType, MethodHandle accessor) {
    throw new AssertionError("not implemented yet: buildObjectArrayWriter for elementType: " + elementType);
  }

  // Container reader factory methods
  static Function<ByteBuffer, Object> createContainerReader(TypeExpr typeExpr, Function<ByteBuffer, Object> elementReader) {
    throw new AssertionError("not implemented yet: createContainerReader for typeExpr: " + typeExpr.toTreeString());
  }

  static Function<ByteBuffer, Object> buildObjectArrayReader(TypeExpr elementType, Class<?> componentType) {
    throw new AssertionError("not implemented yet: buildObjectArrayReader for elementType: " + elementType.toTreeString() + " and componentType: " + componentType.getName());
  }
}
