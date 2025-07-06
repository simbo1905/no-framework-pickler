// SPDX-FileCopyrightText: 2025 Simon Massey  
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.jetbrains.annotations.NotNull;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Array;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
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

  static Function<ByteBuffer, Object> createArrayReader(
      Function<ByteBuffer, Object> elementReader, Class<?> componentType, TypeExpr element) {
    LOGGER.fine(() -> "Creating array reader for component type: " + componentType.getName());
    final int expectedMarker = switch (element) {
      case TypeExpr.RefValueNode(TypeExpr.RefValueType refValueType, Type ignored1) -> switch (refValueType) {
        case BOOLEAN -> Constants.ARRAY_BOOLEAN.marker();
        case BYTE -> Constants.ARRAY_BYTE.marker();
        case SHORT -> Constants.ARRAY_SHORT.marker();
        case CHARACTER -> Constants.ARRAY_CHAR.marker();
        case INTEGER -> Constants.ARRAY_INT.marker();
        case LONG -> Constants.ARRAY_LONG.marker();
        case FLOAT -> Constants.ARRAY_FLOAT.marker();
        case DOUBLE -> Constants.ARRAY_DOUBLE.marker();
        case UUID -> Constants.ARRAY_UUID.marker();
        case ENUM -> Constants.ARRAY_ENUM.marker();
        case STRING -> Constants.ARRAY_STRING.marker();
        case RECORD -> Constants.ARRAY_RECORD.marker();
        case INTERFACE -> Constants.ARRAY_INTERFACE.marker();
      };
      case TypeExpr.ArrayNode(var ignored) -> Constants.ARRAY_ARRAY.marker();
      case TypeExpr.ListNode(var ignored) -> Constants.ARRAY_LIST.marker();
      case TypeExpr.MapNode ignored -> Constants.ARRAY_MAP.marker();
      case TypeExpr.OptionalNode ignored -> Constants.ARRAY_OPTIONAL.marker();
      case TypeExpr.PrimitiveValueNode ignored -> Integer.MIN_VALUE; // should not happen
    };
    return switch (element) {
      case TypeExpr.RefValueNode(TypeExpr.RefValueType ignored, Type ignored1) -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker != expectedMarker) {
          throw new IllegalStateException("Expected marker " + expectedMarker + " but got " + marker);
        }
        int length = ZigZagEncoding.getInt(buffer);
        Object[] array = (Object[]) Array.newInstance(componentType, length);
        Arrays.setAll(array, i -> elementReader.apply(buffer));
        return array;
      };
      default -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker != expectedMarker) {
          throw new IllegalStateException("Expected marker " + expectedMarker + " but got " + marker);
        }
        int length = ZigZagEncoding.getInt(buffer);
        // Use typeExprToClass to get the correct component type for nested arrays
        Class<?> componentTypeInner = typeExprToClass(element);
        Object array = Array.newInstance(componentTypeInner, length);
        for (int i = 0; i < length; i++) {
          Array.set(array, i, elementReader.apply(buffer));
        }
        return array;
      };
    };
  }

  static BiConsumer<ByteBuffer, Object> createMapWriterInner(
      BiConsumer<ByteBuffer, Object> keyWriter,
      BiConsumer<ByteBuffer, Object> valueWriter) {
    return (buffer, obj) -> {
      Map<?, ?> map = (Map<?, ?>) obj;
      final int positionBeforeWrite = buffer.position();
      ZigZagEncoding.putInt(buffer, Constants.MAP.marker());
      ZigZagEncoding.putInt(buffer, map.size());
      LOGGER.fine(() -> "Written map marker " + Constants.MAP.marker() + " and size " + map.size() + " at position " + positionBeforeWrite);
      // Write each key-value pair
      map.forEach((key, value) -> {
        if (key == null) {
          LOGGER.finer(() -> "Map key is null writing NULL_MARKER=-1 marker at position: " + buffer.position());
          buffer.put(NULL_MARKER);
        } else {
          LOGGER.finer(() -> "Map key is present writing NOT_NULL_MARKER=1 marker at position: " + buffer.position());
          buffer.put(NOT_NULL_MARKER);
          keyWriter.accept(buffer, key);
        }
        if (value == null) {
          LOGGER.finer(() -> "Map value is null writing NULL_MARKER=-1 marker at position: " + buffer.position());
          buffer.put(NULL_MARKER); // write a marker for null
        } else {
          LOGGER.finer(() -> "Map value is present writing NOT_NULL_MARKER=1 marker at position: " + buffer.position());
          buffer.put(NOT_NULL_MARKER); // write a marker for non-null
          valueWriter.accept(buffer, value);
        }
      });
    };
  }

  static Function<ByteBuffer, Object> createMapReader(
      Function<ByteBuffer, Object> keyReader,
      Function<ByteBuffer, Object> valueReader) {
    return buffer -> {
      int positionBeforeRead = buffer.position();
      int marker = ZigZagEncoding.getInt(buffer);
      int size = ZigZagEncoding.getInt(buffer);
      LOGGER.fine(() -> "Read map marker " + marker + " and size " + size + " at position " + positionBeforeRead);
      Map<Object, Object> map = new HashMap<>(size);
      IntStream.range(0, size).forEach(i -> {
        final Object key = keyReader.apply(buffer);
        final Object value = valueReader.apply(buffer);
        map.put(key, value);
      });
      return Collections.unmodifiableMap(map);
    };
  }

  static Function<ByteBuffer, Object> createListReader(Function<ByteBuffer, Object> elementReader) {
    return buffer -> {
      int marker = ZigZagEncoding.getInt(buffer);
      assert marker == Constants.LIST.marker() : "Expected LIST marker";
      int size = ZigZagEncoding.getInt(buffer);
      return IntStream.range(0, size)
          .mapToObj(i -> elementReader.apply(buffer))
          .toList();
    };
  }

  static Function<ByteBuffer, Object> createOptionalReader(Function<ByteBuffer, Object> valueReader) {
    return buffer -> {
      int marker = ZigZagEncoding.getInt(buffer);
      if (marker == Constants.OPTIONAL_EMPTY.marker()) {
        return Optional.empty();
      } else if (marker == Constants.OPTIONAL_OF.marker()) {
        return Optional.of(valueReader.apply(buffer));
      } else {
        throw new IllegalStateException("Invalid optional marker: " + marker);
      }
    };
  }

  static BiConsumer<ByteBuffer, Object> createOptionalWriterInner(BiConsumer<ByteBuffer, Object> valueWriter) {
    LOGGER.fine(() -> "Creating optional writer with valueWriter: " + valueWriter);
    return (buffer, value) -> {
      Optional<?> optional = (Optional<?>) value;
      if (optional.isEmpty()) {
        LOGGER.fine(() -> "Optional is empty, writing EMPTY marker at position: " + buffer.position());
        ZigZagEncoding.putInt(buffer, Constants.OPTIONAL_EMPTY.marker());
      } else {
        final int positionBeforeWrite = buffer.position();
        ZigZagEncoding.putInt(buffer, Constants.OPTIONAL_OF.marker());
        buffer.put(NOT_NULL_MARKER);
        LOGGER.fine(() -> "Optional is present wrote OPTIONAL_OF=" + Constants.OPTIONAL_OF.marker() + " followed by NOT_NULL_MARKER=1 at position: " + positionBeforeWrite);
        valueWriter.accept(buffer, optional.get());
      }
    };
  }

  static BiConsumer<ByteBuffer, Object> createListWriterInner(BiConsumer<ByteBuffer, Object> elementWriter) {
    return (buffer, value) -> {
      List<?> list = (List<?>) value;
      // Write LIST marker and size
      ZigZagEncoding.putInt(buffer, Constants.LIST.marker());
      ZigZagEncoding.putInt(buffer, list.size());
      // Write each element
      for (Object item : list) {
        if (item == null) {
          LOGGER.finer(() -> "Extracted value is null writing NULL_MARKER=-1 marker at position: " + buffer.position());
          buffer.put(NULL_MARKER); // Write -1 for null
        } else {
          LOGGER.finer(() -> "Extracted value is present NOT_NULL_MARKER=1 marker at position: " + buffer.position());
          buffer.put(NOT_NULL_MARKER); // Write +1 for non-null
          elementWriter.accept(buffer, item); // Delegate to the actual writer
        }
      }
    };
  }

  static ToIntFunction<Object> createOptionalSizerInner(ToIntFunction<Object> valueSizer) {
    LOGGER.fine(() -> "Creating optional sizer with delegate valueSizer: " + valueSizer);
    return (Object inner) -> {
      Optional<?> optional = (Optional<?>) inner;
      if (optional.isEmpty()) {
        LOGGER.fine(() -> "Optional is empty, returning size 0");
        return Integer.BYTES; // size of the marker only
      } else {
        LOGGER.fine(() -> "Optional is present, calculating size for value");
        final Object value = optional.get();
        return Integer.BYTES + valueSizer.applyAsInt(value); // size of the marker + value size
      }
    };
  }

  static ToIntFunction<Object> createArraySizerInner(ToIntFunction<Object> elementSizer) {
    LOGGER.fine(() -> "Creating array sizer with delegate elementSizer: " + elementSizer);
    return (Object inner) -> {
      if (inner == null) {
        LOGGER.fine(() -> "Array is null, returning size 0");
        return Integer.BYTES; // size of the null marker only
      }
      assert inner.getClass().isArray() : "Expected an array but got: " + inner.getClass().getName();
      final int length = Array.getLength(inner);
      int size = Integer.BYTES + Integer.BYTES; // size of the marker and length of the array
      return size + IntStream.range(0, length).map(i -> {
        Object element = Array.get(inner, i);
        if (element == null) {
          return 1;
        } else {
          return 1 + elementSizer.applyAsInt(element);
        }
      }).sum();
    };
  }

  static ToIntFunction<Object> createListSizerInner(ToIntFunction<Object> elementSizer) {
    LOGGER.fine(() -> "Creating list sizer inner with delegate elementSizer: " + elementSizer);
    return (Object inner) -> {
      if (inner == null) {
        LOGGER.fine(() -> "List is null, returning size 0");
        return Integer.BYTES; // size of the null marker only
      }
      assert inner instanceof List<?> : "Expected a List but got: " + inner.getClass().getName();
      List<?> list = (List<?>) inner;
      int baseSize = Integer.BYTES + Integer.BYTES; // size of the marker and length of the array
      int elementsSize = list.stream().mapToInt(element -> {
        if (element == null) {
          // If the element is null, its size is just the 1-byte null marker
          return Byte.BYTES;
        } else {
          // If not null, its size is the 1-byte not-null marker plus the element's own size
          return Byte.BYTES + elementSizer.applyAsInt(element);
        }
      }).sum();
      return baseSize + elementsSize;
    };
  }

  static ToIntFunction<Object> createMapSizerInner
      (ToIntFunction<Object> keySizer, ToIntFunction<Object> valueSizer) {
    return (Object inner) -> {
      if (inner == null) {
        LOGGER.fine(() -> "Map is null, returning size 0");
        return Integer.BYTES; // size of the null marker only
      }
      assert inner instanceof Map<?, ?> : "Expected a Map but got: " + inner.getClass().getName();
      Map<?, ?> map = (Map<?, ?>) inner;
      int size = Integer.BYTES + Integer.BYTES; // size of the marker and length of the map
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        Object key = entry.getKey();
        Object value = entry.getValue();

        // Add size for the key (marker + data)
        size += Byte.BYTES; // For the key's null/not-null marker
        if (key != null) {
          size += keySizer.applyAsInt(key);
        }

        // Add size for the value (marker + data)
        size += Byte.BYTES; // For the value's null/not-null marker
        if (value != null) {
          size += valueSizer.applyAsInt(value);
        }
      }
      return size;
    };
  }

  static BiConsumer<ByteBuffer, Object> createArrayRefWriter(BiConsumer<ByteBuffer, Object> elementWriter, TypeExpr element) {
    LOGGER.fine(() -> "Creating array writer inner for element type: " + element.toTreeString());
    final int marker = switch (element) {
      case TypeExpr.RefValueNode(TypeExpr.RefValueType refValueType, Type ignored1) -> switch (refValueType) {
        case BOOLEAN -> Constants.ARRAY_BOOLEAN.marker();
        case BYTE -> Constants.ARRAY_BYTE.marker();
        case SHORT -> Constants.ARRAY_SHORT.marker();
        case CHARACTER -> Constants.ARRAY_CHAR.marker();
        case INTEGER -> Constants.ARRAY_INT.marker();
        case LONG -> Constants.ARRAY_LONG.marker();
        case FLOAT -> Constants.ARRAY_FLOAT.marker();
        case DOUBLE -> Constants.ARRAY_DOUBLE.marker();
        case UUID -> Constants.ARRAY_UUID.marker();
        case ENUM -> Constants.ARRAY_ENUM.marker();
        case STRING -> Constants.ARRAY_STRING.marker();
        case RECORD -> Constants.ARRAY_RECORD.marker();
        case INTERFACE -> Constants.ARRAY_INTERFACE.marker();
      };
      case TypeExpr.ArrayNode(var ignored) -> Constants.ARRAY_ARRAY.marker();
      case TypeExpr.ListNode(var ignored) -> Constants.ARRAY_LIST.marker();
      case TypeExpr.MapNode ignored -> Constants.ARRAY_MAP.marker();
      case TypeExpr.OptionalNode ignored -> Constants.ARRAY_OPTIONAL.marker();
      case TypeExpr.PrimitiveValueNode ignored -> Integer.MIN_VALUE; // should not happen
    };
    return (buffer, value) -> {
      Object[] array = (Object[]) value;
      // Write ARRAY_OBJ marker and length
      ZigZagEncoding.putInt(buffer, marker);
      ZigZagEncoding.putInt(buffer, array.length);
      // Write each element
      for (Object item : array) {
        if (item == null) {
          LOGGER.finer(() -> "Array element is null writing NULL_MARKER=-1 marker at position: " + buffer.position());
          buffer.put(NULL_MARKER); // write a marker for null
        } else {
          LOGGER.finer(() -> "Array element is present writing NOT_NULL_MARKER=1 marker at position: " + buffer.position());
          buffer.put(NOT_NULL_MARKER); // write a marker for non-null
          elementWriter.accept(buffer, item);
        }
      }
    };
  }

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

  static @NotNull BiConsumer<ByteBuffer, Object> buildPrimitiveArrayWriterInner(TypeExpr.PrimitiveValueType primitiveType) {
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

  static @NotNull BiConsumer<ByteBuffer, Object> buildPrimitiveValueWriter(TypeExpr.PrimitiveValueType primitiveType) {
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

  Set<Class<?>> BOXED_PRIMITIVES = Set.of(
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

  static ToIntFunction<Object> extractAndDelegate(ToIntFunction<Object> delegate, MethodHandle accessor) {
    return (Object record) -> {
      final Object value;
      try {
        value = accessor.invokeWithArguments(record);
      } catch (Throwable e) {
        throw new RuntimeException(e.getMessage(), e);
      }
      if (value == null) {
        LOGGER.fine(() -> "Extracted value is null, writing 0L marker");
        return Integer.BYTES; // size of the marker only
      } else {
        LOGGER.fine(() -> "Extracted value is not null, delegating to sizer " + delegate.hashCode());
        return delegate.applyAsInt(value);
      }
    };
  }


  static BiConsumer<ByteBuffer, Object> extractAndDelegate(BiConsumer<ByteBuffer, Object> delegate, MethodHandle
      accessor) {
    final Class<?> type = accessor.type().returnType();
    if (type.isPrimitive()) {
      return (buffer, record) -> {
        LOGGER.fine(() -> "Extracting primitive value using accessor: " + accessor + " for record: " + record);
        final Object value;
        try {
          value = accessor.invokeWithArguments(record);
        } catch (Throwable e) {
          throw new RuntimeException(e.getMessage(), e);
        }
        LOGGER.finer(() -> "Extracted ref value cannot be null delegating to writer: " + value);
        delegate.accept(buffer, value);
      };
    } else {
      return (buffer, record) -> {
        LOGGER.fine(() -> "Extracting ref value then will NULL check using accessor: " + accessor + " for record: " + record);
        final Object value;
        try {
          value = accessor.invokeWithArguments(record);
        } catch (Throwable e) {
          throw new RuntimeException(e.getMessage(), e);
        }
        final var positionBefore = buffer.position();
        if (value == null) {
          LOGGER.finer(() -> "Writing NULL_MARKER=-1 marker at position: " + positionBefore);
          buffer.put(NULL_MARKER); // write a marker for null
        } else {
          LOGGER.finer(() -> "Writing NOT_NULL_MARKER=1 marker then delegating to writer for value at position: " + positionBefore);
          buffer.put(NOT_NULL_MARKER); // write a marker for null
          delegate.accept(buffer, value);
        }
      };
    }
  }

  /// Build sizer chain with callback for complex types
  static ToIntFunction<Object> buildSizerChain(TypeExpr typeExpr, MethodHandle accessor,
                                               Function<Class<?>, ToIntFunction<Object>> complexResolver) {
    return extractAndDelegate(buildSizerChainInner(typeExpr, complexResolver), accessor);
  }

  /// Build sizer chain inner with callback delegation
  static ToIntFunction<Object> buildSizerChainInner(TypeExpr typeExpr,
                                                    Function<Class<?>, ToIntFunction<Object>> complexResolver) {
    return switch (typeExpr) {
      case TypeExpr.PrimitiveValueNode(var primitiveType, var ignored) -> buildPrimitiveValueSizer(primitiveType);
      case TypeExpr.RefValueNode(var refValueType, var javaType) ->
          buildValueSizerInner(refValueType, javaType, complexResolver);
      case TypeExpr.ArrayNode(var element) -> {
        if (element instanceof TypeExpr.PrimitiveValueNode(var primitiveType, var ignored)) {
          yield buildPrimitiveArraySizerInner(primitiveType);
        } else {
          final var elementSizer = buildSizerChainInner(element, complexResolver);
          yield createArraySizerInner(elementSizer);
        }
      }
      case TypeExpr.ListNode(var element) -> {
        final var elementSizer = buildSizerChainInner(element, complexResolver);
        yield createListSizerInner(elementSizer);
      }
      case TypeExpr.MapNode(var key, var value) -> {
        final var keySizer = buildSizerChainInner(key, complexResolver);
        final var valueSizer = buildSizerChainInner(value, complexResolver);
        yield createMapSizerInner(keySizer, valueSizer);
      }
      case TypeExpr.OptionalNode(var wrapped) -> {
        final var valueSizer = buildSizerChainInner(wrapped, complexResolver);
        yield createOptionalSizerInner(valueSizer);
      }
    };
  }

  /// Build value sizer with callback for complex types (RECORD, INTERFACE, ENUM)
  static ToIntFunction<Object> buildValueSizerInner(TypeExpr.RefValueType refValueType, Type javaType,
                                                    Function<Class<?>, ToIntFunction<Object>> complexResolver) {
    if (javaType instanceof Class<?> cls) {
      return switch (refValueType) {
        case BOOLEAN, BYTE -> obj -> obj == null ? Byte.BYTES : 2 * Byte.BYTES;
        case SHORT -> obj -> obj == null ? Byte.BYTES : Byte.BYTES + Short.BYTES;
        case CHARACTER -> obj -> obj == null ? Byte.BYTES : Byte.BYTES + Character.BYTES;
        case INTEGER -> obj -> obj == null ? Byte.BYTES : Byte.BYTES + Integer.BYTES;
        case LONG -> obj -> obj == null ? Byte.BYTES : Byte.BYTES + Long.BYTES;
        case FLOAT -> obj -> obj == null ? Byte.BYTES : Byte.BYTES + Float.BYTES;
        case DOUBLE -> obj -> obj == null ? Byte.BYTES : Byte.BYTES + Double.BYTES;
        case UUID -> obj -> obj == null ? Byte.BYTES : Byte.BYTES + 2 * Long.BYTES;
        case STRING -> obj -> {
          if (obj == null) return Byte.BYTES;
          final String str = (String) obj;
          return Byte.BYTES + (str.length() + 1) * Integer.BYTES;
        };
        default -> complexResolver.apply(cls); // Delegate RECORD, INTERFACE, ENUM to callback
      };
    }
    throw new AssertionError("Unsupported Java type: " + javaType);
  }

  /// Build writer chain with callback for complex types
  static BiConsumer<ByteBuffer, Object> buildWriterChain(TypeExpr typeExpr, MethodHandle accessor,
                                                         Function<Class<?>, BiConsumer<ByteBuffer, Object>> complexResolver) {
    return extractAndDelegate(buildWriterChainInner(typeExpr, complexResolver), accessor);
  }

  /// Build writer chain inner with callback delegation
  static BiConsumer<ByteBuffer, Object> buildWriterChainInner(TypeExpr typeExpr,
                                                              Function<Class<?>, BiConsumer<ByteBuffer, Object>> complexResolver) {
    return switch (typeExpr) {
      case TypeExpr.PrimitiveValueNode(var primitiveType, var ignored) -> buildPrimitiveValueWriter(primitiveType);
      case TypeExpr.RefValueNode(var refValueType, var javaType) ->
          buildValueWriter(refValueType, javaType, complexResolver);
      case TypeExpr.ArrayNode(var element) -> {
        if (element instanceof TypeExpr.PrimitiveValueNode(var primitiveType, var ignored)) {
          yield buildPrimitiveArrayWriterInner(primitiveType);
        } else {
          final var elementWriter = buildWriterChainInner(element, complexResolver);
          yield createArrayRefWriter(elementWriter, element);
        }
      }
      case TypeExpr.ListNode(var element) -> {
        final var elementWriter = buildWriterChainInner(element, complexResolver);
        yield createListWriterInner(elementWriter);
      }
      case TypeExpr.MapNode(var key, var value) -> {
        final var keyWriter = buildWriterChainInner(key, complexResolver);
        final var valueWriter = buildWriterChainInner(value, complexResolver);
        yield createMapWriterInner(keyWriter, valueWriter);
      }
      case TypeExpr.OptionalNode(var wrapped) -> {
        final var valueWriter = buildWriterChainInner(wrapped, complexResolver);
        yield createOptionalWriterInner(valueWriter);
      }
    };
  }

  /// Build value writer with callback for complex types
  static BiConsumer<ByteBuffer, Object> buildValueWriter(TypeExpr.RefValueType refValueType, Type javaType,
                                                         Function<Class<?>, BiConsumer<ByteBuffer, Object>> complexResolver) {
    if (javaType instanceof Class<?> cls) {
      return switch (refValueType) {
        case BOOLEAN -> (buffer, obj) -> buffer.put((byte) ((boolean) obj ? 1 : 0));
        case BYTE -> (buffer, obj) -> buffer.put((byte) obj);
        case SHORT -> (buffer, obj) -> buffer.putShort((short) obj);
        case CHARACTER -> (buffer, obj) -> buffer.putChar((char) obj);
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
        case FLOAT -> (buffer, obj) -> buffer.putFloat((float) obj);
        case DOUBLE -> (buffer, obj) -> buffer.putDouble((double) obj);
        case UUID -> (buffer, obj) -> {
          final var uuid = (java.util.UUID) obj;
          buffer.putLong(uuid.getMostSignificantBits());
          buffer.putLong(uuid.getLeastSignificantBits());
        };
        case STRING -> (buffer, obj) -> {
          final String str = (String) obj;
          final byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
          ZigZagEncoding.putInt(buffer, bytes.length);
          buffer.put(bytes);
        };
        default -> complexResolver.apply(cls); // Delegate RECORD, INTERFACE, ENUM to callback
      };
    }
    throw new AssertionError("Unsupported Java type: " + javaType);
  }

  /// Build reader chain with callback for complex types
  static Function<ByteBuffer, Object> buildReaderChain(TypeExpr typeExpr,
                                                       Function<Long, Function<ByteBuffer, Object>> complexResolver) {
    return switch (typeExpr) {
      case TypeExpr.PrimitiveValueNode(var primitiveType, var ignored) -> buildPrimitiveValueReader(primitiveType);
      case TypeExpr.RefValueNode(var refValueType, var ignored) -> buildValueReader(refValueType, complexResolver);
      case TypeExpr.ArrayNode(var element) -> {
        final Function<ByteBuffer, Object> nonNullArrayReader;
        if (element instanceof TypeExpr.PrimitiveValueNode(var primitiveType, var ignored)) {
          nonNullArrayReader = buildPrimitiveArrayReader(primitiveType);
        } else {
          final var elementReader = buildReaderChain(element, complexResolver);
          final var componentType = extractComponentType(element);
          nonNullArrayReader = createArrayReader(elementReader, componentType, element);
        }
        yield nullCheckAndDelegate(nonNullArrayReader);
      }
      case TypeExpr.ListNode(var element) -> {
        final var elementReader = buildReaderChain(element, complexResolver);
        final var nonNullListReader = createListReader(elementReader);
        yield nullCheckAndDelegate(nonNullListReader);
      }
      case TypeExpr.MapNode(var key, var value) -> {
        final var keyReader = buildReaderChain(key, complexResolver);
        final var valueReader = buildReaderChain(value, complexResolver);
        final var nonNullMapReader = createMapReader(keyReader, valueReader);
        yield nullCheckAndDelegate(nonNullMapReader);
      }
      case TypeExpr.OptionalNode(var wrapped) -> {
        final var valueReader = buildReaderChain(wrapped, complexResolver);
        final var nonNullOptionalReader = createOptionalReader(valueReader);
        yield nullCheckAndDelegate(nonNullOptionalReader);
      }
    };
  }

  /// Build value reader with callback for complex types
  static Function<ByteBuffer, Object> buildValueReader(TypeExpr.RefValueType refValueType,
                                                       Function<Long, Function<ByteBuffer, Object>> complexResolver) {
    final Function<ByteBuffer, Object> primitiveReader = buildRefValueReader(refValueType, complexResolver);
    return nullCheckAndDelegate(primitiveReader);
  }

  /// Build ref value reader with callback for complex types
  static Function<ByteBuffer, Object> buildRefValueReader(TypeExpr.RefValueType refValueType,
                                                          Function<Long, Function<ByteBuffer, Object>> complexResolver) {
    return switch (refValueType) {
      case BOOLEAN -> buffer -> buffer.get() != 0;
      case BYTE -> ByteBuffer::get;
      case SHORT -> ByteBuffer::getShort;
      case CHARACTER -> ByteBuffer::getChar;
      case INTEGER -> buffer -> {
        final var positionBefore = buffer.position();
        LOGGER.fine(() -> "RefValueReader reading INTEGER_VAR or INTEGER marker at position: " + positionBefore);
        final int marker = ZigZagEncoding.getInt(buffer);
        if (marker == INTEGER_VAR.marker()) {
          return ZigZagEncoding.getInt(buffer);
        } else if (marker == INTEGER.marker()) {
          return buffer.getInt();
        } else {
          throw new IllegalStateException("Expected INTEGER marker but got: " + marker + " at position: " + positionBefore);
        }
      };
      case LONG -> buffer -> {
        final var positionBefore = buffer.position();
        LOGGER.fine(() -> "RefValueReader reading LONG_VAR or LONG marker at position: " + positionBefore);
        final int marker = ZigZagEncoding.getInt(buffer);
        if (marker == Constants.LONG_VAR.marker()) {
          return ZigZagEncoding.getLong(buffer);
        } else if (marker == Constants.LONG.marker()) {
          return buffer.getLong();
        } else {
          throw new IllegalStateException("Expected LONG marker but got: " + marker + " at position: " + positionBefore);
        }
      };
      case FLOAT -> ByteBuffer::getFloat;
      case DOUBLE -> ByteBuffer::getDouble;
      case UUID -> buffer -> {
        final long most = buffer.getLong();
        final long least = buffer.getLong();
        return new java.util.UUID(most, least);
      };
      case STRING -> buffer -> {
        final int length = ZigZagEncoding.getInt(buffer);
        final byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
      };
      default -> buffer -> {
        final long typeSignature = buffer.getLong();
        return complexResolver.apply(typeSignature).apply(buffer);
      };
    };
  }

  /// Null check and delegate pattern for readers
  static Function<ByteBuffer, Object> nullCheckAndDelegate(Function<ByteBuffer, Object> delegate) {
    return buffer -> {
      final int positionBefore = buffer.position();
      LOGGER.finer(() -> "Reading null marker at position: " + positionBefore);
      final byte nullMarker = buffer.get();
      if (nullMarker == NULL_MARKER) {
        return null;
      } else if (nullMarker == NOT_NULL_MARKER) {
        return delegate.apply(buffer);
      } else {
        throw new IllegalStateException("Invalid null marker: " + nullMarker +
            " at position " + positionBefore);
      }
    };
  }

  /// Witness method for type-safe delegation to other picklers
  @SuppressWarnings("unchecked")
  static <X> int writeToWireWitness(Pickler<X> pickler, ByteBuffer buffer, Object record) {
    return switch (pickler) {
      case RecordSerde<X> rs -> rs.writeToWire(buffer, (X) record);
      case EmptyRecordSerde<?> ers -> {
        buffer.putLong(ers.typeSignature);
        yield Long.BYTES;
      }
      default -> // Fallback for other pickler types
          pickler.serialize(buffer, (X) record);
    };
  }

  /// Compute type signatures for all record classes using streams
  static Map<Class<?>, Long> computeRecordTypeSignatures(List<Class<?>> recordClasses) {
    return recordClasses.stream()
        .collect(Collectors.toMap(
            clz -> clz,
            clz -> {
              final var components = clz.getRecordComponents();
              final var typeExprs = Arrays.stream(components)
                  .map(comp -> TypeExpr.analyzeType(comp.getGenericType()))
                  .toArray(TypeExpr[]::new);
              return hashClassSignature(clz, components, typeExprs);
            }
        ));
  }

  /// Compute a CLASS_SIG_BYTES signature from class name and component metadata
  static long hashClassSignature(Class<?> clazz, RecordComponent[] components, TypeExpr[] componentTypes) {
    String input = Stream.concat(Stream.of(clazz.getSimpleName()), IntStream.range(0, components.length).boxed().flatMap(i -> Stream.concat(Stream.of(componentTypes[i].toTreeString()), Stream.of(components[i].getName())))).collect(Collectors.joining("!"));
    return hashSignature(input);
  }

  /// Compute a CLASS_SIG_BYTES signature from enum class and constant names
  static long hashEnumSignature(Class<?> enumClass) {
    Object[] enumConstants = enumClass.getEnumConstants();
    assert enumConstants != null : "Not an enum class: " + enumClass;
    String input = Stream.concat(Stream.of(enumClass.getSimpleName()), Arrays.stream(enumConstants).map(e -> ((Enum<?>) e).name())).collect(Collectors.joining("!"));
    return hashSignature(input);
  }

  static long hashSignature(String uniqueNess) {
    long result;
    final MessageDigest digest;
    try {
      digest = MessageDigest.getInstance(SHA_256);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e.getMessage(), e);
    }

    byte[] hash = digest.digest(uniqueNess.getBytes(StandardCharsets.UTF_8));

    // Convert first CLASS_SIG_BYTES to long
    //      Byte Index:   0       1       2        3        4        5        6        7
    //      Bits:      [56-63] [48-55] [40-47] [32-39] [24-31] [16-23] [ 8-15] [ 0-7]
    //      Shift:      <<56   <<48   <<40    <<32    <<24    <<16    <<8     <<0
    result = IntStream.range(0, CLASS_SIG_BYTES).mapToLong(i -> (hash[i] & 0xFFL) << (56 - i * 8)).reduce(0L, (a, b) -> a | b);
    return result;
  }
}
