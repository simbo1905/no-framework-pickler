// SPDX-FileCopyrightText: 2025 Simon Massey  
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.jetbrains.annotations.NotNull;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Array;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static io.github.simbo1905.no.framework.TypeExpr.PrimitiveValueType.*;

/// This is a namespace of static functional methods.
sealed interface Companion permits Companion.Nothing {

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

  record Nothing() implements Companion {
  }

  /// Callback interface for circular dependency resolution
  interface DependencyResolver {
    Pickler<?> resolve(Class<?> targetClass);

    /// Resolve pickler by type signature for interface deserialization
    default Pickler<?> resolveBySignature(long typeSignature) {
      throw new UnsupportedOperationException("Type signature resolution not implemented");
    }
  }

  String SHA_256 = "SHA-256";
  int SAMPLE_SIZE = 32;
  /// NULL_MARKER is used as a sentinel value to indicate null when we need to distinguish between null and a positive value (e.g., enum ordinals)
  byte NULL_MARKER = Byte.MIN_VALUE;
  byte NOT_NULL_MARKER = Byte.MAX_VALUE;

  /// Discover all reachable types from a root class including sealed hierarchies and record components
  static Set<Class<?>> recordClassHierarchy(final Class<?> current, Collection<SerdeHandler> customHandlers) {
    return recordClassHierarchyInner(current, new HashSet<>(), customHandlers).collect(Collectors.toSet());
  }

  /// Discover all reachable types from a root class including sealed hierarchies and record components
  static Stream<Class<?>> recordClassHierarchyInner(
      final Class<?> current,
      final Set<Class<?>> visited, Collection<SerdeHandler> customHandlers) {
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
          recordClassHierarchyInner(componentType, visited, customHandlers)
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

                  TypeExpr structure = TypeExpr.analyzeType(component.getGenericType(), customHandlers);
                  LOGGER.finer(() -> "Component " + component.getName() + " discovered types: " +
                      structure.toTreeString());
                  Stream<Class<?>> structureStream = TypeExpr.classesInAST(structure);
                  return Stream.concat(arrayStream, structureStream);
                })
                : Stream.empty()
        ).flatMap(child -> recordClassHierarchyInner(child, visited, customHandlers))
    );
  }


  static Class<?> extractComponentType(TypeExpr typeExpr) {
    // Simplified component type extraction
    return switch (typeExpr) {
      case TypeExpr.RefValueNode(var ignored, var javaType) -> (Class<?>) javaType;
      case TypeExpr.OptionalNode(var ignored) -> Optional.class;
      case TypeExpr.ListNode(var ignored) -> List.class;
      case TypeExpr.ArrayNode(var ignored, var ignored2) -> Object[].class;
      case TypeExpr.MapNode(var ignoredKey, var ignoredValue) -> Map.class;
      default -> Object.class;
    };
  }

  enum Markers {
    BOOLEAN(-1),
    BYTE(-2),
    SHORT(-3),
    CHARACTER(-4),
    INTEGER(-5),
    LONG(-6),
    FLOAT(-7),
    DOUBLE(-8),
    STRING(-9),
    UUID(-10),
    LOCAL_DATE(-11),
    LOCAL_DATE_TIME(-12),
    INTEGER_VAR(-32),
    LONG_VAR(-33),
    OPTIONAL_EMPTY(-30),
    OPTIONAL_OF(-31),
    MAP(-4),
    LIST(-5),
    ARRAY_RECORD(-13),
    ARRAY_INTERFACE(-14),
    ARRAY_ENUM(-15),
    ARRAY_BOOLEAN(-16),
    ARRAY_BYTE(-17),
    ARRAY_SHORT(-18),
    ARRAY_CHAR(-19),
    ARRAY_INT(-20),
    ARRAY_LONG(-21),
    ARRAY_FLOAT(-22),
    ARRAY_DOUBLE(-23),
    ARRAY_STRING(-24),
    ARRAY_UUID(-25),
    ARRAY_LOCAL_DATE(-26),
    ARRAY_LOCAL_DATE_TIME(-27),
    ARRAY_ARRAY(-28),
    ARRAY_LIST(-27),
    ARRAY_MAP(-28),
    ARRAY_OPTIONAL(-29);
    private final int marker;

    Markers(int marker) {
      this.marker = marker;
    }

    public int marker() {
      return marker;
    }
  }

  static Serdes.Reader createArrayReader(
      Serdes.Reader elementReader, Class<?> componentType, TypeExpr element) {
    LOGGER.fine(() -> "Creating array reader for component type: " + componentType.getName() + " and element: " + element.toTreeString());
    final int expectedMarker = switch (element) {
      case TypeExpr.RefValueNode(TypeExpr.RefValueType refValueType, Type ignored1) -> switch (refValueType) {
        case RECORD -> Markers.ARRAY_RECORD.marker();
        case INTERFACE -> Markers.ARRAY_INTERFACE.marker();
        case ENUM -> Markers.ARRAY_ENUM.marker();
        case BOOLEAN -> Markers.ARRAY_BOOLEAN.marker();
        case BYTE -> Markers.ARRAY_BYTE.marker();
        case SHORT -> Markers.ARRAY_SHORT.marker();
        case CHARACTER -> Markers.ARRAY_CHAR.marker();
        case INTEGER -> Markers.ARRAY_INT.marker();
        case LONG -> Markers.ARRAY_LONG.marker();
        case FLOAT -> Markers.ARRAY_FLOAT.marker();
        case DOUBLE -> Markers.ARRAY_DOUBLE.marker();
        case STRING -> Markers.ARRAY_STRING.marker();
        case LOCAL_DATE -> Markers.ARRAY_LOCAL_DATE.marker();
        case LOCAL_DATE_TIME -> Markers.ARRAY_LOCAL_DATE_TIME.marker();
        case CUSTOM -> throw new AssertionError("not implemented");
      };
      case TypeExpr.ArrayNode(var ignored, var ignored2) -> Markers.ARRAY_ARRAY.marker();
      case TypeExpr.ListNode(var ignored) -> Markers.ARRAY_LIST.marker();
      case TypeExpr.MapNode ignored -> Markers.ARRAY_MAP.marker();
      case TypeExpr.OptionalNode ignored -> Markers.ARRAY_OPTIONAL.marker();
      case TypeExpr.PrimitiveArrayNode(var ignored, var ignored2) -> Markers.ARRAY_ARRAY.marker();
      case TypeExpr.PrimitiveValueNode ignored -> Integer.MIN_VALUE; // should not happen
    };
    return switch (element) {
      case TypeExpr.RefValueNode(TypeExpr.RefValueType ignored, Type ignored1) -> buffer -> {
        int posBeforeMarker = buffer.position();
        int marker = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "Reading RefValueNode array marker " + marker + " at position " + posBeforeMarker + " (expected " + expectedMarker + ")");
        if (marker != expectedMarker) {
          throw new IllegalStateException("Expected marker " + expectedMarker + " but got " + marker);
        }
        int posBeforeLength = buffer.position();
        int length = ZigZagEncoding.getInt(buffer);
        LOGGER.fine(() -> "Reading array length " + length + " at position " + posBeforeLength);
        Object[] array = (Object[]) Array.newInstance(componentType, length);
        Arrays.setAll(array, i -> elementReader.apply(buffer));
        return array;
      };
      default -> buffer -> {
        int posBeforeMarker = buffer.position();
        int marker = ZigZagEncoding.getInt(buffer);
        LOGGER.fine(() -> "Reading array marker " + marker + " at position " + posBeforeMarker + " (expected " + expectedMarker + " for " + element.toTreeString() + ")");
        if (marker != expectedMarker) {
          throw new IllegalStateException("Expected marker " + expectedMarker + " but got " + marker);
        }
        int posBeforeLength = buffer.position();
        int length = ZigZagEncoding.getInt(buffer);
        LOGGER.fine(() -> "Reading array length " + length + " at position " + posBeforeLength);
        // Use typeExprToClass to get the correct component type for nested arrays
        Class<?> componentTypeInner = typeExprToClass(element);
        Object array = Array.newInstance(componentTypeInner, length);
        Arrays.setAll((Object[]) array, i -> elementReader.apply(buffer));
        return array;
      };
    };
  }

  static Serdes.Writer createMapWriterInner(
      Serdes.Writer keyWriter,
      Serdes.Writer valueWriter) {
    return (buffer, obj) -> {
      Map<?, ?> map = (Map<?, ?>) obj;
      final int positionBeforeWrite = buffer.position();
      ZigZagEncoding.putInt(buffer, Markers.MAP.marker());
      /// TODO do not write NULL_MARKER if we can use NOT_NULL_MARKER as a sentinel - for maps we could write NULL_MARKER as the size when map is null
      ZigZagEncoding.putInt(buffer, map.size());
      LOGGER.fine(() -> "Written map marker " + Markers.MAP.marker() + " and size " + map.size() + " at position " + positionBeforeWrite);
      // Write each key-value pair
      map.forEach((key, value) -> {
        if (key == null) {
          LOGGER.finest(() -> "Map key is null writing NULL_MARKER=-1 marker at position: " + buffer.position());
          buffer.put(NULL_MARKER);
        } else {
          LOGGER.finest(() -> "Map key is present writing NOT_NULL_MARKER=1 marker at position: " + buffer.position());
          buffer.put(NOT_NULL_MARKER);
          keyWriter.accept(buffer, key);
        }
        if (value == null) {
          LOGGER.finest(() -> "Map value is null writing NULL_MARKER=-1 marker at position: " + buffer.position());
          buffer.put(NULL_MARKER); // write a marker for null
        } else {
          LOGGER.finest(() -> "Map value is present writing NOT_NULL_MARKER=1 marker at position: " + buffer.position());
          buffer.put(NOT_NULL_MARKER); // write a marker for non-null
          valueWriter.accept(buffer, value);
        }
      });
    };
  }

  static Serdes.Reader createMapReader(
      Serdes.Reader keyReader,
      Serdes.Reader valueReader) {
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

  static Serdes.Reader createListReader(Serdes.Reader elementReader) {
    return buffer -> {
      int marker = ZigZagEncoding.getInt(buffer);
      assert marker == Markers.LIST.marker() : "Expected LIST marker";
      int size = ZigZagEncoding.getInt(buffer);
      return IntStream.range(0, size)
          .mapToObj(i -> elementReader.apply(buffer))
          .toList();
    };
  }

  static Serdes.Reader createOptionalReader(Serdes.Reader valueReader) {
    return buffer -> {
      int marker = ZigZagEncoding.getInt(buffer);
      if (marker == Markers.OPTIONAL_EMPTY.marker()) {
        return Optional.empty();
      } else if (marker == Markers.OPTIONAL_OF.marker()) {
        return Optional.of(valueReader.apply(buffer));
      } else {
        throw new IllegalStateException("Invalid optional marker: " + marker);
      }
    };
  }

  static Serdes.Writer createOptionalWriterInner(Serdes.Writer valueWriter) {
    LOGGER.fine(() -> "Creating optional writer with valueWriter: " + valueWriter);
    return (buffer, value) -> {
      Optional<?> optional = (Optional<?>) value;
      if (optional.isEmpty()) {
        LOGGER.fine(() -> "Optional is empty, writing EMPTY marker at position: " + buffer.position());
        ZigZagEncoding.putInt(buffer, Markers.OPTIONAL_EMPTY.marker());
      } else {
        final int positionBeforeWrite = buffer.position();
        ZigZagEncoding.putInt(buffer, Markers.OPTIONAL_OF.marker());
        buffer.put(NOT_NULL_MARKER);
        LOGGER.fine(() -> "Optional is present wrote OPTIONAL_OF=" + Markers.OPTIONAL_OF.marker() + " followed by NOT_NULL_MARKER=1 at position: " + positionBeforeWrite);
        valueWriter.accept(buffer, optional.get());
      }
    };
  }

  static Serdes.Writer createListWriterInner(Serdes.Writer elementWriter) {
    return (buffer, value) -> {
      List<?> list = (List<?>) value;
      // Write LIST marker and size
      ZigZagEncoding.putInt(buffer, Markers.LIST.marker());
      /// TODO do not write NULL_MARKER if we can use NOT_NULL_MARKER as a sentinel - for lists we could write NULL_MARKER as the size when list is null
      ZigZagEncoding.putInt(buffer, list.size());
      // Write each element
      for (Object item : list) {
        if (item == null) {
          buffer.put(NULL_MARKER); // Write -1 for null
        } else {
          buffer.put(NOT_NULL_MARKER); // Write +1 for non-null
          elementWriter.accept(buffer, item); // Delegate to the actual writer
        }
      }
    };
  }

  static Serdes.Sizer createOptionalSizerInner(Serdes.Sizer valueSizer) {
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

  static Serdes.Sizer createArraySizerInner(Serdes.Sizer elementSizer) {
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

  static Serdes.Sizer createListSizerInner(Serdes.Sizer elementSizer) {
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

  static Serdes.Sizer createMapSizerInner
      (Serdes.Sizer keySizer, Serdes.Sizer valueSizer) {
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

  static Serdes.Writer createArrayRefWriter(Serdes.Writer elementWriter, TypeExpr element) {
    LOGGER.fine(() -> "Creating array writer inner for element type: " + element.toTreeString());
    final int marker = switch (element) {
      case TypeExpr.RefValueNode(TypeExpr.RefValueType refValueType, Type ignored1) -> switch (refValueType) {
        case RECORD -> Markers.ARRAY_RECORD.marker();
        case INTERFACE -> Markers.ARRAY_INTERFACE.marker();
        case ENUM -> Markers.ARRAY_ENUM.marker();
        case BOOLEAN -> Markers.ARRAY_BOOLEAN.marker();
        case BYTE -> Markers.ARRAY_BYTE.marker();
        case SHORT -> Markers.ARRAY_SHORT.marker();
        case CHARACTER -> Markers.ARRAY_CHAR.marker();
        case INTEGER -> Markers.ARRAY_INT.marker();
        case LONG -> Markers.ARRAY_LONG.marker();
        case FLOAT -> Markers.ARRAY_FLOAT.marker();
        case DOUBLE -> Markers.ARRAY_DOUBLE.marker();
        case STRING -> Markers.ARRAY_STRING.marker();
        case LOCAL_DATE -> Markers.ARRAY_LOCAL_DATE.marker();
        case LOCAL_DATE_TIME -> Markers.ARRAY_LOCAL_DATE_TIME.marker();
        default -> throw new IllegalStateException("Unexpected value: " + refValueType);
      };
      case TypeExpr.ArrayNode(var ignored, var ignored2) -> Markers.ARRAY_ARRAY.marker();
      case TypeExpr.ListNode(var ignored) -> Markers.ARRAY_LIST.marker();
      case TypeExpr.MapNode ignored -> Markers.ARRAY_MAP.marker();
      case TypeExpr.OptionalNode ignored -> Markers.ARRAY_OPTIONAL.marker();
      case TypeExpr.PrimitiveArrayNode(var ignored, var ignored2) -> Markers.ARRAY_ARRAY.marker();
      case TypeExpr.PrimitiveValueNode ignored -> Integer.MIN_VALUE; // should not happen
    };
    return (buffer, value) -> {
      Object[] array = (Object[]) value;
      // Write ARRAY_OBJ marker and length
      int posBeforeMarker = buffer.position();
      ZigZagEncoding.putInt(buffer, marker);
      LOGGER.finer(() -> "Writing array marker " + marker + " at position " + posBeforeMarker + " for element type: " + element.toTreeString());
      int posBeforeLength = buffer.position();
      /// TODO do not write NULL_MARKER if we can use NOT_NULL_MARKER as a sentinel - for arrays we could write NULL_MARKER as the length when array is null
      ZigZagEncoding.putInt(buffer, array.length);
      LOGGER.finer(() -> "Writing array length " + array.length + " at position " + posBeforeLength);
      // Write each element
      for (int i = 0; i < array.length; i++) {
        Object item = array[i];
        if (item == null) {
          final int index = i;
          int posBeforeNull = buffer.position();
          buffer.put(NULL_MARKER); // write a marker for null
          LOGGER.finest(() -> "Writing NULL_MARKER " + NULL_MARKER + " at position " + posBeforeNull + " for array element " + index);
        } else {
          final int index = i;
          int posBeforeNotNull = buffer.position();
          buffer.put(NOT_NULL_MARKER); // write a marker for non-null
          LOGGER.finest(() -> "Writing NOT_NULL_MARKER " + NOT_NULL_MARKER + " at position " + posBeforeNotNull + " for array element " + index);
          elementWriter.accept(buffer, item);
        }
      }
    };
  }

  /// Build component serdes with lazy callback resolution
  static ComponentSerde[] buildComponentSerdesWithCallback(
      Class<?> recordClass,
      DependencyResolver resolver,
      List<SerdeHandler> customHandlers) {
    LOGGER.fine(() -> "Building ComponentSerde[] with callback for record: " + recordClass.getName());

    RecordComponent[] components = recordClass.getRecordComponents();
    MethodHandle[] getters = resolveGetters(recordClass, components);

    return IntStream.range(0, components.length)
        .mapToObj(i -> {
          RecordComponent component = components[i];
          MethodHandle getter = getters[i];
          TypeExpr typeExpr = TypeExpr.analyzeType(component.getGenericType(), customHandlers);

          LOGGER.finer(() -> "Component " + i + " (" + component.getName() + "): " + typeExpr.toTreeString());

          Serdes.Writer writer = createLazyWriter(typeExpr, getter, resolver);
          Serdes.Reader reader = createLazyReader(typeExpr, resolver);
          Serdes.Sizer sizer = createLazySizer(typeExpr, getter, resolver);

          return new ComponentSerde(writer, reader, sizer);
        })
        .toArray(ComponentSerde[]::new);
  }

  /// Build primitive array writer for TypeExpr.PrimitiveValueType (old version)
  static @NotNull Serdes.Writer buildPrimitiveArrayWriterInner(TypeExpr.PrimitiveValueType primitiveType) {
    LOGGER.fine(() -> "Building writer chain for primitive array type: " + primitiveType);
    return switch (primitiveType) {
      case BOOLEAN -> (buffer, inner) -> {
        final var position = buffer.position();
        final var booleans = (boolean[]) inner;
        ZigZagEncoding.putInt(buffer, Markers.BOOLEAN.marker());
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
        ZigZagEncoding.putInt(buffer, Markers.BYTE.marker());
        ZigZagEncoding.putInt(buffer, bytes.length);
        buffer.put(bytes);
      };
      case SHORT -> (buffer, inner) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag SHORT at position " + buffer.position());
        ZigZagEncoding.putInt(buffer, Markers.SHORT.marker());
        final var shorts = (short[]) inner;
        ZigZagEncoding.putInt(buffer, shorts.length);
        for (short s : shorts) {
          buffer.putShort(s);
        }
      };
      case CHARACTER -> (buffer, inner) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag CHARACTER at position " + buffer.position());
        final var chars = (char[]) inner;
        ZigZagEncoding.putInt(buffer, Markers.CHARACTER.marker());
        ZigZagEncoding.putInt(buffer, chars.length);
        for (char c : chars) {
          buffer.putChar(c);
        }
      };
      case FLOAT -> (buffer, inner) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag FLOAT at position " + buffer.position());
        final var floats = (float[]) inner;
        ZigZagEncoding.putInt(buffer, Markers.FLOAT.marker());
        ZigZagEncoding.putInt(buffer, floats.length);
        for (float f : floats) {
          buffer.putFloat(f);
        }
      };
      case DOUBLE -> (buffer, inner) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag DOUBLE at position " + buffer.position());
        ZigZagEncoding.putInt(buffer, Markers.DOUBLE.marker());
        final var doubles = (double[]) inner;
        ZigZagEncoding.putInt(buffer, doubles.length);
        for (double d : doubles) {
          buffer.putDouble(d);
        }
      };
      case INTEGER -> (buffer, inner) -> {
        LOGGER.finer(() -> "Writing INTEGER array at position " + buffer.position() + " with inner type: " + inner.getClass().getSimpleName());
        final var integers = (int[]) inner;
        final var length = Array.getLength(inner);
        final var sampleAverageSize = length > 0 ? estimateAverageSizeInt(integers, length) : 1;
        // Here we must be saving one byte per integer to justify the encoding cost
        if (sampleAverageSize < Integer.BYTES - 1) {
          LOGGER.finer(() -> "Writing INTEGER_VAR marker " + Markers.INTEGER_VAR.marker() + " at position " + buffer.position() + " length=" + length);
          ZigZagEncoding.putInt(buffer, Markers.INTEGER_VAR.marker());
          ZigZagEncoding.putInt(buffer, length);
          IntStream.range(0, integers.length).forEach(i -> {
            LOGGER.finer(() -> "Writing INTEGER_VAR element " + i + " value=" + integers[i] + " at position " + buffer.position());
            ZigZagEncoding.putInt(buffer, integers[i]);
          });
        } else {
          LOGGER.finer(() -> "Writing INTEGER marker " + Markers.INTEGER.marker() + " at position " + buffer.position() + " length=" + length);
          ZigZagEncoding.putInt(buffer, Markers.INTEGER.marker());
          ZigZagEncoding.putInt(buffer, length);
          IntStream.range(0, integers.length).forEach(i -> {
            LOGGER.finer(() -> "Writing INTEGER element " + i + " value=" + integers[i] + " at position " + buffer.position());
            buffer.putInt(integers[i]);
          });
        }
      };
      case LONG -> (buffer, inner) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag LONG at position " + buffer.position());
        final var longs = (long[]) inner;
        final var length = Array.getLength(inner);
        final var sampleAverageSize = length > 0 ? estimateAverageSizeLong(longs, length) : 1;
        if ((length <= SAMPLE_SIZE && sampleAverageSize < Long.BYTES - 1) || (length > SAMPLE_SIZE && sampleAverageSize < Long.BYTES - 2)) {
          LOGGER.fine(() -> "Writing LONG_VAR array - position=" + buffer.position() + " length=" + length);
          ZigZagEncoding.putInt(buffer, Markers.LONG_VAR.marker());
          ZigZagEncoding.putInt(buffer, length);
          for (long i : longs) {
            ZigZagEncoding.putLong(buffer, i);
          }
        } else {
          LOGGER.fine(() -> "Writing LONG array - position=" + buffer.position() + " length=" + length);
          ZigZagEncoding.putInt(buffer, Markers.LONG.marker());
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

  /// Build primitive value writer for TypeExpr.PrimitiveValueType
  static @NotNull Serdes.Writer buildPrimitiveValueWriter(TypeExpr.PrimitiveValueType primitiveType) {
    return switch (primitiveType) {
      case BOOLEAN -> (ByteBuffer buffer, Object result) -> buffer.put((byte) ((boolean) result ? 1 : 0));
      case BYTE -> (ByteBuffer buffer, Object result) -> buffer.put((byte) result);
      case SHORT -> (ByteBuffer buffer, Object result) -> buffer.putShort((short) result);
      case CHARACTER -> (ByteBuffer buffer, Object result) -> buffer.putChar((char) result);
      case INTEGER -> (ByteBuffer buffer, Object object) -> {
        final int result = (int) object;
        final var position = buffer.position();
        if (ZigZagEncoding.sizeOf(result) < Integer.BYTES) {
          LOGGER.fine(() -> "Writing primitive INTEGER_VAR marker=" + Markers.INTEGER_VAR.marker() + " value=" + result + " at position: " + position);
          ZigZagEncoding.putInt(buffer, Markers.INTEGER_VAR.marker());
          ZigZagEncoding.putInt(buffer, result);
        } else {
          LOGGER.fine(() -> "Writing primitive marker=" + Markers.INTEGER_VAR.marker() + "  INTEGER value=" + result + " at position: " + position);
          ZigZagEncoding.putInt(buffer, Markers.INTEGER.marker());
          buffer.putInt(result);
        }
      };
      case LONG -> {
        LOGGER.fine(() -> "Building writer chain for long.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          final long result = (long) record;
          final var position = buffer.position();
          if (ZigZagEncoding.sizeOf(result) < Long.BYTES) {
            ZigZagEncoding.putInt(buffer, Markers.LONG_VAR.marker());
            LOGGER.fine(() -> "Writing LONG_VAR value=" + result + " at position: " + position);
            ZigZagEncoding.putLong(buffer, result);
          } else {
            ZigZagEncoding.putInt(buffer, Markers.LONG.marker());
            LOGGER.finer(() -> "Writing LONG value=" + result + " at position: " + position);
            buffer.putLong(result);
          }
        };
      }
      case FLOAT -> (ByteBuffer buffer, Object inner) -> buffer.putFloat((Float) inner);
      case DOUBLE -> (ByteBuffer buffer, Object inner) -> buffer.putDouble((Double) inner);
    };
  }

  static @NotNull Serdes.Reader buildPrimitiveValueReader(TypeExpr.PrimitiveValueType primitiveType) {
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
        if (marker == Markers.INTEGER_VAR.marker()) {
          int value = ZigZagEncoding.getInt(buffer);
          LOGGER.finer(() -> "INTEGER reader: read INTEGER_VAR value=" + value + " at position=" + buffer.position());
          return value;
        } else if (marker == Markers.INTEGER.marker()) {
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
        if (marker == Markers.LONG_VAR.marker()) {
          LOGGER.finer(() -> "LONG reader: read LONG_VAR marker=" + marker + " at position=" + position);
          return ZigZagEncoding.getLong(buffer);
        } else if (marker == Markers.LONG.marker()) {
          LOGGER.finer(() -> "LONG reader: read LONG marker=" + marker + " at position=" + position);
          return buffer.getLong();
        } else {
          throw new IllegalStateException("Expected LONG or LONG_VAR marker but got: " + marker + " at position: " + position);
        }
      };
    };
  }

  static @NotNull Serdes.Reader buildPrimitiveArrayReader(TypeExpr.PrimitiveValueType primitiveType) {
    return switch (primitiveType) {
      case BOOLEAN -> (buffer) -> {
        int marker = ZigZagEncoding.getInt(buffer);
        assert marker == Markers.BOOLEAN.marker() : "Expected BOOLEAN marker but got: " + marker;
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
        assert marker == Markers.BYTE.marker() : "Expected BYTE marker but got: " + marker;
        int length = ZigZagEncoding.getInt(buffer);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
      };
      case SHORT -> (buffer) -> {
        int marker = ZigZagEncoding.getInt(buffer);
        assert marker == Markers.SHORT.marker() : "Expected SHORT marker but got: " + marker;
        int length = ZigZagEncoding.getInt(buffer);
        short[] shorts = new short[length];
        IntStream.range(0, length).forEach(i -> shorts[i] = buffer.getShort());
        return shorts;
      };
      case CHARACTER -> (buffer) -> {
        int marker = ZigZagEncoding.getInt(buffer);
        assert marker == Markers.CHARACTER.marker() : "Expected CHARACTER marker but got: " + marker;
        int length = ZigZagEncoding.getInt(buffer);
        char[] chars = new char[length];
        IntStream.range(0, length).forEach(i -> chars[i] = buffer.getChar());
        return chars;
      };
      case FLOAT -> (buffer) -> {
        int marker = ZigZagEncoding.getInt(buffer);
        assert marker == Markers.FLOAT.marker() : "Expected FLOAT marker but got: " + marker;
        int length = ZigZagEncoding.getInt(buffer);
        float[] floats = new float[length];
        IntStream.range(0, length).forEach(i -> floats[i] = buffer.getFloat());
        return floats;
      };
      case DOUBLE -> (buffer) -> {
        int marker = ZigZagEncoding.getInt(buffer);
        assert marker == Markers.DOUBLE.marker() : "Expected DOUBLE marker but got: " + marker;
        int length = ZigZagEncoding.getInt(buffer);
        double[] doubles = new double[length];
        IntStream.range(0, length).forEach(i -> doubles[i] = buffer.getDouble());
        return doubles;
      };
      case INTEGER -> (buffer) -> {
        int positionBeforeMarker = buffer.position();
        int marker = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "Reading INTEGER array marker " + marker + " at position " + positionBeforeMarker);
        if (marker == Markers.INTEGER_VAR.marker()) {
          int length = ZigZagEncoding.getInt(buffer);
          LOGGER.finer(() -> "Reading INTEGER_VAR array with length " + length + " at position " + buffer.position());
          int[] integers = new int[length];
          IntStream.range(0, length).forEach(i -> {
            int positionBeforeElement = buffer.position();
            integers[i] = ZigZagEncoding.getInt(buffer);
            LOGGER.finer(() -> "Read INTEGER_VAR element " + i + " value=" + integers[i] + " from position " + positionBeforeElement);
          });
          return integers;
        } else if (marker == Markers.INTEGER.marker()) {
          int length = ZigZagEncoding.getInt(buffer);
          LOGGER.finer(() -> "Reading INTEGER array with length " + length + " at position " + buffer.position());
          int[] integers = new int[length];
          IntStream.range(0, length).forEach(i -> {
            int positionBeforeElement = buffer.position();
            integers[i] = buffer.getInt();
            LOGGER.finer(() -> "Read INTEGER element " + i + " value=" + integers[i] + " from position " + positionBeforeElement);
          });
          return integers;
        } else throw new IllegalStateException("Expected INTEGER or INTEGER_VAR marker but got: " + marker);
      };
      case LONG -> (buffer) -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker == Markers.LONG_VAR.marker()) {
          int length = ZigZagEncoding.getInt(buffer);
          long[] longs = new long[length];
          IntStream.range(0, length).forEach(i -> longs[i] = ZigZagEncoding.getLong(buffer));
          return longs;
        } else if (marker == Markers.LONG.marker()) {
          int length = ZigZagEncoding.getInt(buffer);
          long[] longs = new long[length];
          IntStream.range(0, length).forEach(i -> longs[i] = buffer.getLong());
          return longs;
        } else throw new IllegalStateException("Expected LONG or LONG_VAR marker but got: " + marker);
      };
    };
  }

  static @NotNull Serdes.Sizer buildPrimitiveArraySizer(TypeExpr.PrimitiveValueType primitiveType) {
    if (primitiveType == BOOLEAN) {
      return (Object value) -> 2 * Integer.BYTES + Array.getLength(value) * Byte.BYTES;
    } else if (primitiveType == BYTE) {
      return (Object value) -> 2 * Integer.BYTES + Array.getLength(value) * Byte.BYTES;
    } else if (primitiveType == SHORT) {
      return (Object value) -> 2 * Integer.BYTES + Array.getLength(value) * Short.BYTES;
    } else if (primitiveType == CHARACTER) {
      return (Object value) -> 2 * Integer.BYTES + Array.getLength(value) * Character.BYTES;
    } else if (primitiveType == INTEGER) {
      return (Object value) -> 2 * Integer.BYTES + Array.getLength(value) * Integer.BYTES;
    } else if (primitiveType == LONG) {
      return (Object value) -> 2 * Integer.BYTES + Array.getLength(value) * Long.BYTES;
    } else if (primitiveType == FLOAT) {
      return (Object value) -> 2 * Integer.BYTES + Array.getLength(value) * Float.BYTES;
    } else if (primitiveType == DOUBLE) {
      return (Object value) -> 2 * Integer.BYTES + Array.getLength(value) * Double.BYTES;
    } else {
      throw new IllegalArgumentException("Unsupported primitive type: " + primitiveType);
    }
  }

  static @NotNull Serdes.Sizer buildValueSizer(TypeExpr.RefValueType refValueType, Type javaType, Serdes.SizerResolver typeSizerResolver) {
    if (javaType instanceof Class<?> cls) {
      return switch (refValueType) {
        case BOOLEAN, BYTE -> (Object record) -> Byte.BYTES;
        case SHORT -> (Object record) -> Short.BYTES;
        case CHARACTER -> (Object record) -> Character.BYTES;
        case INTEGER -> (Object record) -> Integer.BYTES;
        case LONG -> (Object record) -> Long.BYTES;
        case FLOAT -> (Object record) -> Float.BYTES;
        case DOUBLE -> (Object record) -> Double.BYTES;
        case STRING -> (Object record) -> {
          final String str = (String) record;
          final byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
          return Integer.BYTES + bytes.length;
        };
        case LOCAL_DATE -> (Object record) -> 3 * Integer.BYTES; // year, month, day
        case LOCAL_DATE_TIME -> (Object record) -> 7 * Integer.BYTES; // year, month, day, hour, minute, second, nano
        case CUSTOM, RECORD, INTERFACE, ENUM -> typeSizerResolver.apply(cls);
      };
    }
    throw new AssertionError("Unsupported Java type: " + javaType);
  }

  static @NotNull Serdes.Sizer buildPrimitiveValueSizer(TypeExpr.PrimitiveValueType primitiveType) {
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

  static @NotNull Serdes.Sizer buildPrimitiveArraySizerInner(TypeExpr.PrimitiveValueType primitiveType) {
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
      case TypeExpr.ArrayNode(var element, var ignored) -> {
        Class<?> componentClass = typeExprToClass(element);
        yield Array.newInstance(componentClass, 0).getClass();
      }
      case TypeExpr.RefValueNode(var ignored, var javaType) -> (Class<?>) javaType;
      case TypeExpr.PrimitiveValueNode(var ignored, var javaType) -> (Class<?>) javaType;
      case TypeExpr.ListNode ignored -> List.class;
      case TypeExpr.MapNode ignored -> Map.class;
      case TypeExpr.OptionalNode ignored -> Optional.class;
      case TypeExpr.PrimitiveArrayNode(var ignored, var javaType) -> javaType;
    };
  }

  static Serdes.Sizer extractAndDelegate(Serdes.Sizer delegate, MethodHandle accessor) {
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

  static Serdes.Writer extractAndDelegate(Serdes.Writer delegate, MethodHandle
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
      /// FIXME this is not being smart enough to see that if it is an array or enum we can use the NULL_MARKER sentinel trick
      ///  to not need to write the NOT_NULL_MARKER we should be able to see from static analysis if it is a top level enum of array
      ///  to do that optimisation
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
          buffer.put(NOT_NULL_MARKER); // write a marker for not-null
          delegate.accept(buffer, value);
        }
      };
    }
  }

  /// Extract and delegate for Writer interface  
  static Serdes.Writer extractAndDelegateWriter(Serdes.Writer delegate, MethodHandle accessor) {
    return extractAndDelegate(delegate, accessor);
  }

  /// Build sizer chain with callback for complex types
  static Serdes.Sizer buildSizerChain(TypeExpr typeExpr, MethodHandle accessor,
                                      Serdes.SizerResolver complexResolver) {
    return extractAndDelegate(buildSizerChainInner(typeExpr, complexResolver), accessor);
  }

  /// Build sizer chain inner with callback delegation
  static Serdes.Sizer buildSizerChainInner(TypeExpr typeExpr,
                                           Serdes.SizerResolver complexResolver) {
    return switch (typeExpr) {
      case TypeExpr.PrimitiveValueNode(var primitiveType, var ignored) -> buildPrimitiveValueSizer(primitiveType);
      case TypeExpr.RefValueNode(var refValueType, var javaType) ->
          buildValueSizerInner(refValueType, javaType, complexResolver);
      case TypeExpr.ArrayNode(var element, var ignored) -> {
        if (element instanceof TypeExpr.PrimitiveValueNode(var primitiveType, var ignored2)) {
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
      default -> throw new IllegalStateException("Unexpected value: " + typeExpr);
    };
  }

  /// Build value sizer with callback for complex types (RECORD, INTERFACE, ENUM)
  static Serdes.Sizer buildValueSizerInner(TypeExpr.RefValueType refValueType, Type javaType,
                                           Serdes.SizerResolver complexResolver) {
    if (javaType instanceof Class<?> cls) {
      return switch (refValueType) {
        case BOOLEAN, BYTE -> obj -> obj == null ? Byte.BYTES : 2 * Byte.BYTES;
        case SHORT -> obj -> obj == null ? Byte.BYTES : Byte.BYTES + Short.BYTES;
        case CHARACTER -> obj -> obj == null ? Byte.BYTES : Byte.BYTES + Character.BYTES;
        case INTEGER -> obj -> obj == null ? Byte.BYTES : Byte.BYTES + Integer.BYTES;
        case LONG -> obj -> obj == null ? Byte.BYTES : Byte.BYTES + Long.BYTES;
        case FLOAT -> obj -> obj == null ? Byte.BYTES : Byte.BYTES + Float.BYTES;
        case DOUBLE -> obj -> obj == null ? Byte.BYTES : Byte.BYTES + Double.BYTES;
        case STRING -> obj -> {
          if (obj == null) return Byte.BYTES;
          final String str = (String) obj;
          return Byte.BYTES + (str.length() + 1) * Integer.BYTES;
        };
        case LOCAL_DATE -> obj -> obj == null ? Byte.BYTES : 4 * Integer.BYTES; // not null + 3
        case LOCAL_DATE_TIME -> obj -> obj == null ? Byte.BYTES : 8 * Integer.BYTES; // not null + 7
        default -> complexResolver.apply(cls); // Delegate RECORD, INTERFACE, ENUM to callback
      };
    }
    throw new AssertionError("Unsupported Java type: " + javaType);
  }

  /// Build writer chain with callback for complex types
  static Serdes.Writer buildWriterChain(TypeExpr typeExpr, MethodHandle accessor,
                                        Serdes.WriterResolver complexResolver) {
    return extractAndDelegate(buildWriterChainInner(typeExpr, complexResolver), accessor);
  }

  /// Build writer chain inner with callback delegation
  static Serdes.Writer buildWriterChainInner(TypeExpr typeExpr,
                                             Serdes.WriterResolver complexResolver) {
    return switch (typeExpr) {
      case TypeExpr.PrimitiveValueNode(var primitiveType, var ignored) -> buildPrimitiveValueWriter(primitiveType);
      case TypeExpr.RefValueNode(var refValueType, var javaType) ->
          buildValueWriter(refValueType, javaType, complexResolver);
      case TypeExpr.ArrayNode(var element, var ignored) -> {
        if (element instanceof TypeExpr.PrimitiveValueNode(var primitiveType, var ignored2)) {
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
      default -> throw new IllegalStateException("Unexpected value: " + typeExpr);
    };
  }

  /// Build value writer with callback for complex types
  static Serdes.Writer buildValueWriter(TypeExpr.RefValueType refValueType, Type javaType,
                                        Serdes.WriterResolver complexResolver) {
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
            ZigZagEncoding.putInt(buffer, Markers.INTEGER_VAR.marker());
            LOGGER.fine(() -> "Writing INTEGER_VAR value=" + result + " at position: " + position);
            ZigZagEncoding.putInt(buffer, result);
          } else {
            ZigZagEncoding.putInt(buffer, Markers.INTEGER.marker());
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
              ZigZagEncoding.putInt(buffer, Markers.LONG_VAR.marker());
              LOGGER.fine(() -> "Writing LONG_VAR value=" + result + " at position: " + position);
              ZigZagEncoding.putLong(buffer, result);
            } else {
              ZigZagEncoding.putInt(buffer, Markers.LONG.marker());
              LOGGER.finer(() -> "Writing LONG value=" + result + " at position: " + position);
              buffer.putLong(result);
            }
          };
        }
        case FLOAT -> (buffer, obj) -> buffer.putFloat((float) obj);
        case DOUBLE -> (buffer, obj) -> buffer.putDouble((double) obj);
        case LOCAL_DATE -> (buffer, obj) -> {
          final var localDate = (LocalDate) obj;
          final int year = localDate.getYear();
          final int month = localDate.getMonthValue();
          final int day = localDate.getDayOfMonth();
          ZigZagEncoding.putInt(buffer, year);
          ZigZagEncoding.putInt(buffer, month);
          ZigZagEncoding.putInt(buffer, day);
        };
        case LOCAL_DATE_TIME -> (buffer, obj) -> {
          final var ldt = (LocalDateTime) obj;
          ZigZagEncoding.putInt(buffer, ldt.getYear());
          ZigZagEncoding.putInt(buffer, ldt.getMonthValue());
          ZigZagEncoding.putInt(buffer, ldt.getDayOfMonth());
          ZigZagEncoding.putInt(buffer, ldt.getHour());
          ZigZagEncoding.putInt(buffer, ldt.getMinute());
          ZigZagEncoding.putInt(buffer, ldt.getSecond());
          ZigZagEncoding.putInt(buffer, ldt.getNano());
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
  static Serdes.Reader buildReaderChain(TypeExpr typeExpr,
                                        Serdes.SignatureReader complexResolver) {
    return switch (typeExpr) {
      case TypeExpr.PrimitiveValueNode(var primitiveType, var ignored) -> buildPrimitiveValueReader(primitiveType);
      case TypeExpr.RefValueNode(var refValueType, var ignored) -> buildValueReader(refValueType, complexResolver);
      case TypeExpr.PrimitiveArrayNode(var primitiveType, var ignored) -> buildPrimitiveArrayReader(primitiveType);
      case TypeExpr.ArrayNode(var element, var ignored3) -> {
        final Serdes.Reader nonNullArrayReader;
        if (element instanceof TypeExpr.PrimitiveValueNode(var primitiveType, var ignored2)) {
          nonNullArrayReader = buildPrimitiveArrayReader(primitiveType);
        } else if (element instanceof TypeExpr.PrimitiveArrayNode(var primitiveType, var ignored2)) {
          // For nested primitive arrays (e.g., int[][] where element is int[])
          final var innerArrayReader = buildPrimitiveArrayReader(primitiveType);
          final var nullCheckedInnerReader = nullCheckAndDelegate(innerArrayReader);
          final var componentType = extractComponentType(element);
          nonNullArrayReader = createArrayReader(nullCheckedInnerReader, componentType, element);
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
  static Serdes.Reader buildValueReader(TypeExpr.RefValueType refValueType,
                                        Serdes.SignatureReader complexResolver) {
    final Serdes.Reader primitiveReader = buildRefValueReader(refValueType, complexResolver);
    return nullCheckAndDelegate(primitiveReader);
  }

  /// Build ref value reader with callback for complex types
  static Serdes.Reader buildRefValueReader(TypeExpr.RefValueType refValueType,
                                           Serdes.SignatureReader complexResolver) {
    return switch (refValueType) {
      case BOOLEAN -> buffer -> buffer.get() != 0;
      case BYTE -> ByteBuffer::get;
      case SHORT -> ByteBuffer::getShort;
      case CHARACTER -> ByteBuffer::getChar;
      case INTEGER -> buffer -> {
        final var positionBefore = buffer.position();
        LOGGER.fine(() -> "RefValueReader reading INTEGER_VAR or INTEGER marker at position: " + positionBefore);
        final int marker = ZigZagEncoding.getInt(buffer);
        if (marker == Markers.INTEGER_VAR.marker()) {
          return ZigZagEncoding.getInt(buffer);
        } else if (marker == Markers.INTEGER.marker()) {
          return buffer.getInt();
        } else {
          throw new IllegalStateException("Expected INTEGER marker but got: " + marker + " at position: " + positionBefore);
        }
      };
      case LONG -> buffer -> {
        final var positionBefore = buffer.position();
        LOGGER.fine(() -> "RefValueReader reading LONG_VAR or LONG marker at position: " + positionBefore);
        final int marker = ZigZagEncoding.getInt(buffer);
        if (marker == Markers.LONG_VAR.marker()) {
          return ZigZagEncoding.getLong(buffer);
        } else if (marker == Markers.LONG.marker()) {
          return buffer.getLong();
        } else {
          throw new IllegalStateException("Expected LONG marker but got: " + marker + " at position: " + positionBefore);
        }
      };
      case FLOAT -> ByteBuffer::getFloat;
      case DOUBLE -> ByteBuffer::getDouble;
      case STRING -> buffer -> {
        final int length = ZigZagEncoding.getInt(buffer);
        final byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
      };
      case LOCAL_DATE -> buffer -> {
        final int year = ZigZagEncoding.getInt(buffer);
        final int month = ZigZagEncoding.getInt(buffer);
        final int day = ZigZagEncoding.getInt(buffer);
        return LocalDate.of(year, month, day);
      };
      case LOCAL_DATE_TIME -> buffer -> {
        final int year = ZigZagEncoding.getInt(buffer);
        final int month = ZigZagEncoding.getInt(buffer);
        final int day = ZigZagEncoding.getInt(buffer);
        final int hour = ZigZagEncoding.getInt(buffer);
        final int minute = ZigZagEncoding.getInt(buffer);
        final int second = ZigZagEncoding.getInt(buffer);
        final int nano = ZigZagEncoding.getInt(buffer);
        return LocalDateTime.of(year, month, day, hour, minute, second, nano);
      };
      default -> buffer -> {
        final long typeSignature = buffer.getLong();
        return complexResolver.apply(typeSignature).apply(buffer);
      };
    };
  }

  /// Null check and delegate pattern for readers
  static Serdes.Reader nullCheckAndDelegate(Serdes.Reader delegate) {
    return buffer -> {
      final int positionBefore = buffer.position();
      final byte nullMarker = buffer.get();
      LOGGER.finest(() -> "Read null marker " + nullMarker + " at position " + positionBefore +
          " (NULL=" + NULL_MARKER + ", NOT_NULL=" + NOT_NULL_MARKER + ")");
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

  /// Compute type signatures for all record classes using streams using the generic type information
  static Map<Class<?>, Long> computeRecordTypeSignatures(List<Class<?>> recordClasses, Collection<SerdeHandler> customHandlers) {
    return recordClasses.stream()
        .filter(Class::isRecord) // Only process actual record classes
        .collect(Collectors.toMap(
            clz -> clz,
            clz -> {
              final var components = clz.getRecordComponents();
              final var typeExprs = Arrays.stream(components)
                  .map(comp -> TypeExpr.analyzeType(comp.getGenericType(), customHandlers))
                  .toArray(TypeExpr[]::new);
              final var signature = hashClassSignature(clz, components, typeExprs);
              LOGGER.finer(() -> "Computed type signature for " + clz.getName() + ": 0x" + Long.toHexString(signature));
              return signature;
            }
        ));
  }

  /// Compute a type signature from full class name, the component types, and component name
  static long hashClassSignature(Class<?> clazz, RecordComponent[] components, TypeExpr[] componentTypes) {
    String input = Stream.concat(
            Stream.concat(Stream.of(clazz.getName()), Stream.of(String.valueOf(components.length))),
            IntStream.range(0, components.length).boxed().flatMap(i ->
                Stream.concat(Stream.of(componentTypes[i].toTreeString()), Stream.of(components[i].getName()))))
        .collect(Collectors.joining("!"));
    final var signature = hashSignature(input);
    LOGGER.fine(() -> "RECORD Signature for " + clazz.getName() + " input '" + input + "' -> 0x" + Long.toHexString(signature));
    return signature;
  }

  /// Compute a type signature from enum class full name and constant names
  static long hashEnumSignature(Class<?> enumClass) {
    Object[] enumConstants = enumClass.getEnumConstants();
    assert enumConstants != null : "Not an enum class: " + enumClass;
    String input = Stream.concat(Stream.of(enumClass.getName()), Arrays.stream(enumConstants).map(e -> ((Enum<?>) e).name())).collect(Collectors.joining("!"));
    final var signature = hashSignature(input);
    LOGGER.fine(() -> "ENUM Signature for " + enumClass.getName() + " input '" + input + "' -> 0x" + Long.toHexString(signature));
    return signature;
  }

  /// This method computes a 64 bit signature from a unique string representation by hashing it using SHA-256
  /// then extracting the first `Long.BYTES` big endian bytes into a long.
  /// INTERNAL USE ONLY - Use hashClassSignature() or hashEnumSignature() instead
  static long hashSignature(String uniqueNess) {
    final MessageDigest digest;
    try {
      digest = MessageDigest.getInstance(SHA_256);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
    final byte[] b = digest.digest(uniqueNess.getBytes(StandardCharsets.UTF_8));
    final var bb = ByteBuffer.wrap(b).order(ByteOrder.BIG_ENDIAN);
    // XOR 16 bytes down to 8 bytes
    return bb.getLong() ^ bb.getLong();
  }

  /// Create lazy sizer that uses callback for record types
  static Serdes.Sizer createLazySizer(TypeExpr typeExpr, MethodHandle getter, DependencyResolver resolver) {
    final Serdes.Sizer sizerChain = createLazySizerChain(typeExpr, resolver);
    return extractAndDelegate(sizerChain, getter);
  }

  /// Create lazy writer that uses callback for record types
  static Serdes.Writer createLazyWriter(TypeExpr typeExpr, MethodHandle getter, DependencyResolver resolver) {
    final Serdes.Writer writerChain = createLazyWriterChain(typeExpr, resolver);
    return extractAndDelegateWriter(writerChain, getter);
  }

  /// Create lazy reader that uses callback for record types
  static Serdes.Reader createLazyReader(TypeExpr typeExpr, DependencyResolver resolver) {
    return createLazyReaderChain(typeExpr, resolver);
  }

  /// Create lazy sizer chain with callback delegation
  static Serdes.Sizer createLazySizerChain(TypeExpr typeExpr, DependencyResolver resolver) {
    return switch (typeExpr) {
      case TypeExpr.PrimitiveValueNode(var primitiveType, var ignored) -> buildPrimitiveValueSizer(primitiveType);
      case TypeExpr.PrimitiveArrayNode(var primitiveType, var ignored) -> buildPrimitiveArraySizer(primitiveType);
      case TypeExpr.RefValueNode(var refValueType, var javaType) -> {
        Class<?> clazz = (Class<?>) javaType;
        if (clazz.isRecord()) {
          yield obj -> {
            if (obj == null) return Byte.BYTES;
            @SuppressWarnings("unchecked") final var pickler = (Pickler<Object>) resolver.resolve(clazz);
            return Byte.BYTES + pickler.maxSizeOf(obj);
          };
        } else if (clazz.isEnum()) {
          yield obj -> {
            if (obj == null) return Byte.BYTES;
            @SuppressWarnings("unchecked") final var pickler = (Pickler<Object>) resolver.resolve(clazz);
            return Byte.BYTES + pickler.maxSizeOf(obj);
          };
        } else if (clazz.isInterface()) {
          yield obj -> {
            if (obj == null) return Byte.BYTES;
            @SuppressWarnings("unchecked") final var pickler = (Pickler<Object>) resolver.resolve(obj.getClass());
            return Byte.BYTES + pickler.maxSizeOf(obj);
          };
        } else {
          yield buildValueSizerInner(refValueType, javaType, cls -> {
            @SuppressWarnings("unchecked") final var pickler = (Pickler<Object>) resolver.resolve(cls);
            return pickler::maxSizeOf;
          });
        }
      }
      case TypeExpr.ArrayNode(var element, var ignored) -> {
        final var elementSizer = createLazySizerChain(element, resolver);
        yield createArraySizerInner(elementSizer);
      }
      case TypeExpr.ListNode(var element) -> {
        final var elementSizer = createLazySizerChain(element, resolver);
        yield createListSizerInner(elementSizer);
      }
      case TypeExpr.MapNode(var key, var value) -> {
        final var keySizer = createLazySizerChain(key, resolver);
        final var valueSizer = createLazySizerChain(value, resolver);
        yield createMapSizerInner(keySizer, valueSizer);
      }
      case TypeExpr.OptionalNode(var wrapped) -> {
        final var valueSizer = createLazySizerChain(wrapped, resolver);
        yield createOptionalSizerInner(valueSizer);
      }
    };
  }

  /// Create lazy writer chain with callback delegation
  static Serdes.Writer createLazyWriterChain(TypeExpr typeExpr, DependencyResolver resolver) {
    return switch (typeExpr) {
      case TypeExpr.PrimitiveValueNode(var primitiveType, var ignored) -> buildPrimitiveValueWriter(primitiveType);
      case TypeExpr.PrimitiveArrayNode(var primitiveType, var ignored) -> buildPrimitiveArrayWriterInner(primitiveType);
      case TypeExpr.RefValueNode(var refValueType, var javaType) -> {
        Class<?> clazz = (Class<?>) javaType;
        if (clazz.isRecord()) {
          yield (buffer, obj) -> {
            @SuppressWarnings("unchecked") final var pickler = (Pickler<Object>) resolver.resolve(clazz);
            pickler.serialize(buffer, obj);
          };
        } else if (clazz.isEnum()) {
          yield (buffer, obj) -> {
            @SuppressWarnings("unchecked") final var pickler = (Pickler<Object>) resolver.resolve(clazz);
            pickler.serialize(buffer, obj);
          };
        } else if (clazz.isInterface()) {
          yield (buffer, obj) -> {
            @SuppressWarnings("unchecked") final var pickler = (Pickler<Object>) resolver.resolve(obj.getClass());
            pickler.serialize(buffer, obj);
          };
        } else {
          yield buildValueWriter(refValueType, javaType, cls -> {
            @SuppressWarnings("unchecked") final var pickler = (Pickler<Object>) resolver.resolve(cls);
            return pickler::serialize;
          });
        }
      }
      case TypeExpr.ArrayNode(var element, var ignored) -> {
        final var elementWriter = createLazyWriterChain(element, resolver);
        yield createArrayRefWriter(elementWriter, element);
      }
      case TypeExpr.ListNode(var element) -> {
        final var elementWriter = createLazyWriterChain(element, resolver);
        yield createListWriterInner(elementWriter);
      }
      case TypeExpr.MapNode(var key, var value) -> {
        final var keyWriter = createLazyWriterChain(key, resolver);
        final var valueWriter = createLazyWriterChain(value, resolver);
        yield createMapWriterInner(keyWriter, valueWriter);
      }
      case TypeExpr.OptionalNode(var wrapped) -> {
        final var valueWriter = createLazyWriterChain(wrapped, resolver);
        yield createOptionalWriterInner(valueWriter);
      }
    };
  }

  /// Create lazy reader chain with callback delegation
  static Serdes.Reader createLazyReaderChain(TypeExpr typeExpr, DependencyResolver resolver) {
    return switch (typeExpr) {
      case TypeExpr.PrimitiveValueNode(var primitiveType, var ignored) -> buildPrimitiveValueReader(primitiveType);
      case TypeExpr.PrimitiveArrayNode(var primitiveType, var ignored) -> {
        final var primitiveArrayReader = buildPrimitiveArrayReader(primitiveType);
        yield nullCheckAndDelegate(primitiveArrayReader);
      }
      case TypeExpr.RefValueNode(var refValueType, var javaType) -> {
        Class<?> clazz = (Class<?>) javaType;
        if (clazz.isRecord()) {
          final Serdes.Reader recordReader = buffer -> {
            buffer.getLong();
            final var pickler = resolver.resolve(clazz);
            return switch (pickler) {
              case RecordSerde<?> recordSerde -> recordSerde.deserializeWithoutSignature(buffer);
              case EmptyRecordSerde<?> emptyRecordSerde -> emptyRecordSerde.deserializeWithoutSignature(buffer);
              case EnumSerde<?> enumPickler -> enumPickler.deserializeWithoutSignature(buffer);
              case null, default ->
                  throw new IllegalStateException("Unsupported serde type: " + (pickler != null ? pickler.getClass() : null));
            };
          };
          yield nullCheckAndDelegate(recordReader);
        } else if (clazz.isEnum()) {
          final Serdes.Reader enumReader = buffer -> {
            buffer.getLong();
            final var pickler = resolver.resolve(clazz);
            if (pickler instanceof EnumSerde<?> enumPickler) {
              return enumPickler.deserializeWithoutSignature(buffer);
            } else {
              throw new IllegalStateException("Expected EnumSerde but got: " + pickler.getClass());
            }
          };
          yield nullCheckAndDelegate(enumReader);
        } else if (clazz.isInterface()) {
          final Serdes.Reader interfaceReader = buffer -> {
            final long typeSignature = buffer.getLong();
            // For interfaces, we need to resolve the concrete type at runtime
            final var pickler = resolver.resolveBySignature(typeSignature);
            return switch (pickler) {
              case RecordSerde<?> recordSerde -> recordSerde.deserializeWithoutSignature(buffer);
              case EmptyRecordSerde<?> emptyRecordSerde -> emptyRecordSerde.deserializeWithoutSignature(buffer);
              case EnumSerde<?> enumPickler -> enumPickler.deserializeWithoutSignature(buffer);
              case null, default ->
                  throw new IllegalStateException("Unsupported serde type: " + (pickler != null ? pickler.getClass() : null));
            };
          };
          yield nullCheckAndDelegate(interfaceReader);
        } else {
          if (refValueType == TypeExpr.RefValueType.CUSTOM) {
            final Serdes.Reader customReader = buffer -> {
              final var pickler = resolver.resolve(clazz);
              return pickler.deserialize(buffer);
            };
            yield nullCheckAndDelegate(customReader);
          }
          // The old 'else' block remains for built-in types
          yield buildValueReader(refValueType, typeSignature -> {
            // For non-record types, use the existing buildRefValueReader approach
            final var pickler = resolver.resolveBySignature(typeSignature);
            return pickler::deserialize;
          });
        }
      }
      case TypeExpr.ArrayNode(var element, var componentType) -> {
        final Serdes.Reader elementReader;
        if (element instanceof TypeExpr.PrimitiveArrayNode(var primitiveType, var ignored2)) {
          final var primitiveArrayReader = buildPrimitiveArrayReader(primitiveType);
          elementReader = nullCheckAndDelegate(primitiveArrayReader);
        } else {
          elementReader = createLazyReaderChain(element, resolver);
        }
        final var nonNullArrayReader = createArrayReader(elementReader, componentType, element);
        yield nullCheckAndDelegate(nonNullArrayReader);
      }
      case TypeExpr.ListNode(var element) -> {
        final var elementReader = createLazyReaderChain(element, resolver);
        final var nonNullListReader = createListReader(elementReader);
        yield nullCheckAndDelegate(nonNullListReader);
      }
      case TypeExpr.MapNode(var key, var value) -> {
        final var keyReader = createLazyReaderChain(key, resolver);
        final var valueReader = createLazyReaderChain(value, resolver);
        final var nonNullMapReader = createMapReader(keyReader, valueReader);
        yield nullCheckAndDelegate(nonNullMapReader);
      }
      case TypeExpr.OptionalNode(var wrapped) -> {
        final var valueReader = createLazyReaderChain(wrapped, resolver);
        final var nonNullOptionalReader = createOptionalReader(valueReader);
        yield nullCheckAndDelegate(nonNullOptionalReader);
      }
    };
  }

  /// Resolve getters for record components
  static MethodHandle[] resolveGetters(Class<?> recordClass, RecordComponent[] components) {
    MethodHandles.Lookup lookup = MethodHandles.publicLookup();
    return IntStream.range(0, components.length)
        .mapToObj(i -> {
          try {
            return lookup.unreflect(components[i].getAccessor());
          } catch (IllegalAccessException e) {
            throw new IllegalStateException("Cannot access component " + i + " of " + recordClass, e);
          }
        })
        .toArray(MethodHandle[]::new);
  }
}
