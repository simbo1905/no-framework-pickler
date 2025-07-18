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
import static io.github.simbo1905.no.framework.TypeExpr2.PrimitiveValueType.*;

/// This is the static helpers of the pickler
sealed interface Companion permits Companion.Nothing {

  record Nothing() implements Companion {
  }

  /// Callback interface for circular dependency resolution
  @FunctionalInterface
  interface DependencyResolver {
    Pickler<?> resolve(Class<?> targetClass);
  }

  String SHA_256 = "SHA-256";
  int SAMPLE_SIZE = 32;
  /// NULL_MARKER is used as a sentinel value to indicate null when we need to distinguish between null and a positive value (e.g., enum ordinals)
  byte NULL_MARKER = Byte.MIN_VALUE;
  byte NOT_NULL_MARKER = Byte.MAX_VALUE;
  /// FFL is a mask constant used to isolate the least significant byte (0xFF) as a long value.
  /// When used with bitwise AND (&), it ensures that byte values are treated as unsigned
  /// (0-255) rather than signed (-128 to 127) when converting to long. This is crucial
  /// for the hash signature calculation where we need to treat each byte of the SHA-256
  /// hash as an unsigned value before bit-shifting and combining them into a long.
  long FFL = 0xFFL;

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

                  TypeExpr2 structure = TypeExpr2.analyzeType(component.getGenericType(), Set.of());
                  LOGGER.finer(() -> "Component " + component.getName() + " discovered types: " +
                      structure.toTreeString());
                  Stream<Class<?>> structureStream = TypeExpr2.classesInAST(structure);
                  return Stream.concat(arrayStream, structureStream);
                })
                : Stream.empty()
        ).flatMap(child -> recordClassHierarchyInner(child, visited))
    );
  }


  static Class<?> extractComponentType(TypeExpr2 typeExpr) {
    // Simplified component type extraction
    return switch (typeExpr) {
      case TypeExpr2.RefValueNode(var ignored, var javaType) -> (Class<?>) javaType;
      case TypeExpr2.OptionalNode(var ignored) -> Optional.class;
      case TypeExpr2.ListNode(var ignored) -> List.class;
      case TypeExpr2.ArrayNode(var ignored, var ignored2) -> Object[].class;
      case TypeExpr2.MapNode(var ignoredKey, var ignoredValue) -> Map.class;
      default -> Object.class;
    };
  }

  /// Get marker for a primitive type
  static int primitiveToMarker(Class<?> primitiveClass) {
    return switch (primitiveClass) {
      case Class<?> c when c == boolean.class -> -2;
      case Class<?> c when c == byte.class -> -3;
      case Class<?> c when c == short.class -> -4;
      case Class<?> c when c == char.class -> -5;
      case Class<?> c when c == int.class -> -6;
      case Class<?> c when c == long.class -> -8;
      case Class<?> c when c == float.class -> -10;
      case Class<?> c when c == double.class -> -11;
      default -> throw new IllegalArgumentException("Not a primitive type: " + primitiveClass);
    };
  }

  /// Get marker for built-in reference types
  static int referenceToMarker(Class<?> refClass) {
    return switch (refClass) {
      case Class<?> c when c == Boolean.class -> -2;
      case Class<?> c when c == Byte.class -> -3;
      case Class<?> c when c == Short.class -> -4;
      case Class<?> c when c == Character.class -> -5;
      case Class<?> c when c == Integer.class -> -6;
      case Class<?> c when c == Long.class -> -8;
      case Class<?> c when c == Float.class -> -10;
      case Class<?> c when c == Double.class -> -11;
      case Class<?> c when c == String.class -> -12;
      case Class<?> c when c == LocalDate.class -> -13;
      case Class<?> c when c == LocalDateTime.class -> -14;
      case Class<?> c when c == Enum.class -> -15;
      default -> throw new IllegalArgumentException("Not a built-in reference type: " + refClass);
    };
  }

  enum Constants2 {
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

    Constants2(int marker) {
      this.marker = marker;
    }

    public int marker() {
      return marker;
    }
  }

  static Reader createArrayReader(
      Reader elementReader, Class<?> componentType, TypeExpr2 element) {
    LOGGER.fine(() -> "Creating array reader for component type: " + componentType.getName() + " and element: " + element.toTreeString());
    final int expectedMarker = switch (element) {
      case TypeExpr2.RefValueNode(TypeExpr2.RefValueType refValueType, Type ignored1) -> switch (refValueType) {
        case RECORD -> Constants2.ARRAY_RECORD.marker();
        case INTERFACE -> Constants2.ARRAY_INTERFACE.marker();
        case ENUM -> Constants2.ARRAY_ENUM.marker();
        case BOOLEAN -> Constants2.ARRAY_BOOLEAN.marker();
        case BYTE -> Constants2.ARRAY_BYTE.marker();
        case SHORT -> Constants2.ARRAY_SHORT.marker();
        case CHARACTER -> Constants2.ARRAY_CHAR.marker();
        case INTEGER -> Constants2.ARRAY_INT.marker();
        case LONG -> Constants2.ARRAY_LONG.marker();
        case FLOAT -> Constants2.ARRAY_FLOAT.marker();
        case DOUBLE -> Constants2.ARRAY_DOUBLE.marker();
        case STRING -> Constants2.ARRAY_STRING.marker();
        case LOCAL_DATE -> Constants2.ARRAY_LOCAL_DATE.marker();
        case LOCAL_DATE_TIME -> Constants2.ARRAY_LOCAL_DATE_TIME.marker();
        case CUSTOM -> throw new AssertionError("not implemented");
      };
      case TypeExpr2.ArrayNode(var ignored, var ignored2) -> Constants2.ARRAY_ARRAY.marker();
      case TypeExpr2.ListNode(var ignored) -> Constants2.ARRAY_LIST.marker();
      case TypeExpr2.MapNode ignored -> Constants2.ARRAY_MAP.marker();
      case TypeExpr2.OptionalNode ignored -> Constants2.ARRAY_OPTIONAL.marker();
      case TypeExpr2.PrimitiveArrayNode(var ignored, var ignored2) -> Constants2.ARRAY_ARRAY.marker();
      case TypeExpr2.PrimitiveValueNode ignored -> Integer.MIN_VALUE; // should not happen
      default -> throw new AssertionError("Unexpected value: " + element);
    };
    return switch (element) {
      case TypeExpr2.RefValueNode(TypeExpr2.RefValueType ignored, Type ignored1) -> buffer -> {
        int posBeforeMarker = buffer.position();
        int marker = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "Reading RefValueNode array marker " + marker + " at position " + posBeforeMarker + " (expected " + expectedMarker + ")");
        if (marker != expectedMarker) {
          throw new IllegalStateException("Expected marker " + expectedMarker + " but got " + marker);
        }
        int posBeforeLength = buffer.position();
        int length = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "Reading array length " + length + " at position " + posBeforeLength);
        Object[] array = (Object[]) Array.newInstance(componentType, length);
        Arrays.setAll(array, i -> {
          LOGGER.finer(() -> "Reading array element " + i + " at position " + buffer.position());
          return elementReader.apply(buffer);
        });
        return array;
      };
      default -> buffer -> {
        int posBeforeMarker = buffer.position();
        int marker = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "Reading array marker " + marker + " at position " + posBeforeMarker + " (expected " + expectedMarker + " for " + element.toTreeString() + ")");
        if (marker != expectedMarker) {
          throw new IllegalStateException("Expected marker " + expectedMarker + " but got " + marker);
        }
        int posBeforeLength = buffer.position();
        int length = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "Reading array length " + length + " at position " + posBeforeLength);
        // Use typeExprToClass to get the correct component type for nested arrays
        Class<?> componentTypeInner = typeExprToClass(element);
        Object array = Array.newInstance(componentTypeInner, length);
        for (int i = 0; i < length; i++) {
          final int index = i;
          LOGGER.finer(() -> "Reading array element " + index + " at position " + buffer.position());
          Array.set(array, i, elementReader.apply(buffer));
        }
        return array;
      };
    };
  }

  static Writer createMapWriterInner(
      Writer keyWriter,
      Writer valueWriter) {
    return (buffer, obj) -> {
      Map<?, ?> map = (Map<?, ?>) obj;
      final int positionBeforeWrite = buffer.position();
      ZigZagEncoding.putInt(buffer, Constants2.MAP.marker());
      /// TODO do not write NULL_MARKER if we can use NOT_NULL_MARKER as a sentinel - for maps we could write NULL_MARKER as the size when map is null
      ZigZagEncoding.putInt(buffer, map.size());
      LOGGER.fine(() -> "Written map marker " + Constants2.MAP.marker() + " and size " + map.size() + " at position " + positionBeforeWrite);
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

  static Reader createMapReader(
      Reader keyReader,
      Reader valueReader) {
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

  static Reader createListReader(Reader elementReader) {
    return buffer -> {
      int marker = ZigZagEncoding.getInt(buffer);
      assert marker == Constants2.LIST.marker() : "Expected LIST marker";
      int size = ZigZagEncoding.getInt(buffer);
      return IntStream.range(0, size)
          .mapToObj(i -> elementReader.apply(buffer))
          .toList();
    };
  }

  static Reader createOptionalReader(Reader valueReader) {
    return buffer -> {
      int marker = ZigZagEncoding.getInt(buffer);
      if (marker == Constants2.OPTIONAL_EMPTY.marker()) {
        return Optional.empty();
      } else if (marker == Constants2.OPTIONAL_OF.marker()) {
        return Optional.of(valueReader.apply(buffer));
      } else {
        throw new IllegalStateException("Invalid optional marker: " + marker);
      }
    };
  }

  static Writer createOptionalWriterInner(Writer valueWriter) {
    LOGGER.fine(() -> "Creating optional writer with valueWriter: " + valueWriter);
    return (buffer, value) -> {
      Optional<?> optional = (Optional<?>) value;
      if (optional.isEmpty()) {
        LOGGER.fine(() -> "Optional is empty, writing EMPTY marker at position: " + buffer.position());
        ZigZagEncoding.putInt(buffer, Constants2.OPTIONAL_EMPTY.marker());
      } else {
        final int positionBeforeWrite = buffer.position();
        ZigZagEncoding.putInt(buffer, Constants2.OPTIONAL_OF.marker());
        buffer.put(NOT_NULL_MARKER);
        LOGGER.fine(() -> "Optional is present wrote OPTIONAL_OF=" + Constants2.OPTIONAL_OF.marker() + " followed by NOT_NULL_MARKER=1 at position: " + positionBeforeWrite);
        valueWriter.accept(buffer, optional.get());
      }
    };
  }

  static Writer createListWriterInner(Writer elementWriter) {
    return (buffer, value) -> {
      List<?> list = (List<?>) value;
      // Write LIST marker and size
      ZigZagEncoding.putInt(buffer, Constants2.LIST.marker());
      /// TODO do not write NULL_MARKER if we can use NOT_NULL_MARKER as a sentinel - for lists we could write NULL_MARKER as the size when list is null
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

  static Sizer createOptionalSizerInner(Sizer valueSizer) {
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

  static Sizer createArraySizerInner(Sizer elementSizer) {
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

  static Sizer createListSizerInner(Sizer elementSizer) {
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

  static Sizer createMapSizerInner
      (Sizer keySizer, Sizer valueSizer) {
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

  static Writer createArrayRefWriter(Writer elementWriter, TypeExpr2 element) {
    LOGGER.fine(() -> "Creating array writer inner for element type: " + element.toTreeString());
    final int marker = switch (element) {
      case TypeExpr2.RefValueNode(TypeExpr2.RefValueType refValueType, Type ignored1) -> switch (refValueType) {
        case RECORD -> Constants2.ARRAY_RECORD.marker();
        case INTERFACE -> Constants2.ARRAY_INTERFACE.marker();
        case ENUM -> Constants2.ARRAY_ENUM.marker();
        case BOOLEAN -> Constants2.ARRAY_BOOLEAN.marker();
        case BYTE -> Constants2.ARRAY_BYTE.marker();
        case SHORT -> Constants2.ARRAY_SHORT.marker();
        case CHARACTER -> Constants2.ARRAY_CHAR.marker();
        case INTEGER -> Constants2.ARRAY_INT.marker();
        case LONG -> Constants2.ARRAY_LONG.marker();
        case FLOAT -> Constants2.ARRAY_FLOAT.marker();
        case DOUBLE -> Constants2.ARRAY_DOUBLE.marker();
        case STRING -> Constants2.ARRAY_STRING.marker();
        case LOCAL_DATE -> Constants2.ARRAY_LOCAL_DATE.marker();
        case LOCAL_DATE_TIME -> Constants2.ARRAY_LOCAL_DATE_TIME.marker();
        default -> throw new IllegalStateException("Unexpected value: " + refValueType);
      };
      case TypeExpr2.ArrayNode(var ignored, var ignored2) -> Constants2.ARRAY_ARRAY.marker();
      case TypeExpr2.ListNode(var ignored) -> Constants2.ARRAY_LIST.marker();
      case TypeExpr2.MapNode ignored -> Constants2.ARRAY_MAP.marker();
      case TypeExpr2.OptionalNode ignored -> Constants2.ARRAY_OPTIONAL.marker();
      case TypeExpr2.PrimitiveArrayNode(var ignored, var ignored2) -> Constants2.ARRAY_ARRAY.marker();
      case TypeExpr2.PrimitiveValueNode ignored -> Integer.MIN_VALUE; // should not happen
      default -> throw new AssertionError("Unexpected value: " + element);
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
          LOGGER.finer(() -> "Writing NULL_MARKER " + NULL_MARKER + " at position " + posBeforeNull + " for array element " + index);
        } else {
          final int index = i;
          int posBeforeNotNull = buffer.position();
          buffer.put(NOT_NULL_MARKER); // write a marker for non-null
          LOGGER.finer(() -> "Writing NOT_NULL_MARKER " + NOT_NULL_MARKER + " at position " + posBeforeNotNull + " for array element " + index);
          elementWriter.accept(buffer, item);
        }
      }
    };
  }

  /// Build ComponentSerde array for a record type using TypeExpr AST
  static ComponentSerde[] buildComponentSerdes(
      Class<?> recordClass,
      Collection<SerdeHandler> customHandlers,
      SizerResolver typeSizerResolver,
      WriterResolver typeWriterResolver,
      ReaderResolver typeReaderResolver
  ) {
    LOGGER.fine(() -> "Building ComponentSerde[] for record: " + recordClass.getName());

    // Pre-process custom handlers into efficient lookup maps
    final Map<Class<?>, Sizer> customSizers = customHandlers.stream()
        .collect(Collectors.toMap(SerdeHandler::valueBasedLike, SerdeHandler::sizer));
    final Map<Class<?>, Writer> customWriters = customHandlers.stream()
        .collect(Collectors.toMap(SerdeHandler::valueBasedLike, SerdeHandler::writer));
    final Map<Class<?>, Reader> customReaders = customHandlers.stream()
        .collect(Collectors.toMap(SerdeHandler::valueBasedLike, SerdeHandler::reader));
    final Map<Class<?>, Integer> customMarkers = customHandlers.stream()
        .collect(Collectors.toMap(SerdeHandler::valueBasedLike, SerdeHandler::marker));

    RecordComponent[] components = recordClass.getRecordComponents();
    MethodHandle[] getters = resolveGetters(recordClass, components);

    return IntStream.range(0, components.length)
        .mapToObj(i -> {
          RecordComponent component = components[i];
          MethodHandle getter = getters[i];
          TypeExpr2 typeExpr = TypeExpr2.analyzeType(component.getGenericType(), customHandlers);

          LOGGER.finer(() -> "Component " + i + " (" + component.getName() + "): " + typeExpr.toTreeString());

          Writer writer = createComponentWriter(typeExpr, getter, customWriters, customMarkers, typeWriterResolver);
          Reader reader = createComponentReader(typeExpr, customReaders, customMarkers, typeReaderResolver);
          Sizer sizer = createComponentSizer(typeExpr, getter, customSizers, customMarkers, typeSizerResolver);

          return new ComponentSerde(writer, reader, sizer);
        })
        .toArray(ComponentSerde[]::new);
  }

  /// Build component serdes with lazy callback resolution
  static ComponentSerde[] buildComponentSerdesWithCallback(
      Class<?> recordClass,
      DependencyResolver resolver
  ) {
    LOGGER.fine(() -> "Building ComponentSerde[] with callback for record: " + recordClass.getName());

    RecordComponent[] components = recordClass.getRecordComponents();
    MethodHandle[] getters = resolveGetters(recordClass, components);

    return IntStream.range(0, components.length)
        .mapToObj(i -> {
          RecordComponent component = components[i];
          MethodHandle getter = getters[i];
          TypeExpr2 typeExpr = TypeExpr2.analyzeType(component.getGenericType(), List.of());

          LOGGER.finer(() -> "Component " + i + " (" + component.getName() + "): " + typeExpr.toTreeString());

          Sizer sizer = createLazySizer(typeExpr, getter, resolver);
          Writer writer = createLazyWriter(typeExpr, getter, resolver);
          Reader reader = createLazyReader(typeExpr, resolver);

          return new ComponentSerde(writer, reader, sizer);
        })
        .toArray(ComponentSerde[]::new);
  }

  static Sizer createComponentSizer(TypeExpr2 typeExpr, MethodHandle getter, Map<Class<?>, Sizer> customSizers, Map<Class<?>, Integer> customMarkers, SizerResolver typeSizerResolver) {
    LOGGER.fine(() -> "Creating component sizer for: " + typeExpr.toTreeString());

    Sizer sizerChain = createSizerChain(typeExpr, customSizers, customMarkers, typeSizerResolver);

    return extractAndDelegate(sizerChain, getter);
  }

  static Sizer createSizerChain(TypeExpr2 typeExpr, Map<Class<?>, Sizer> customSizers, Map<Class<?>, Integer> customMarkers, SizerResolver typeSizerResolver) {
    return switch (typeExpr) {
      case TypeExpr2.PrimitiveValueNode(var primitiveType, var ignored) -> buildPrimitiveValueSizer(primitiveType);
      case TypeExpr2.PrimitiveArrayNode(var primitiveType, var ignored) -> buildPrimitiveArraySizer(primitiveType);
      case TypeExpr2.RefValueNode(var refValueType, var javaType) -> {
        Class<?> clazz = (Class<?>) javaType;
        if (customSizers.containsKey(clazz)) {
          yield customSizers.get(clazz);
        } else {
          yield buildValueSizer(refValueType, javaType, typeSizerResolver);
        }
      }
      case TypeExpr2.ArrayNode(var element, var componentType) -> {
        final var elementSizer = createSizerChain(element, customSizers, customMarkers, typeSizerResolver);
        yield createArraySizerInner(elementSizer);
      }
      case TypeExpr2.ListNode(var element) -> {
        final var elementSizer = createSizerChain(element, customSizers, customMarkers, typeSizerResolver);
        yield createListSizerInner(elementSizer);
      }
      case TypeExpr2.MapNode(var key, var value) -> {
        final var keySizer = createSizerChain(key, customSizers, customMarkers, typeSizerResolver);
        final var valueSizer = createSizerChain(value, customSizers, customMarkers, typeSizerResolver);
        yield createMapSizerInner(keySizer, valueSizer);
      }
      case TypeExpr2.OptionalNode(var wrapped) -> {
        final var valueSizer = createSizerChain(wrapped, customSizers, customMarkers, typeSizerResolver);
        yield createOptionalSizerInner(valueSizer);
      }
    };
  }

  static Reader createComponentReader(TypeExpr2 typeExpr, Map<Class<?>, Reader> customReaders, Map<Class<?>, Integer> customMarkers, ReaderResolver typeReaderResolver) {
    LOGGER.fine(() -> "Creating component reader for: " + typeExpr.toTreeString());

    return createReaderChain(typeExpr, customReaders, customMarkers, typeReaderResolver);
  }

  static Reader createReaderChain(TypeExpr2 typeExpr, Map<Class<?>, Reader> customReaders, Map<Class<?>, Integer> customMarkers, ReaderResolver typeReaderResolver) {
    return switch (typeExpr) {
      case TypeExpr2.PrimitiveValueNode(var primitiveType, var ignored) -> buildPrimitiveValueReader(primitiveType);
      case TypeExpr2.PrimitiveArrayNode(var primitiveType, var ignored) -> {
        // Primitive arrays at the top level (e.g., int[] as a record component) need null checking
        final var primitiveArrayReader = buildPrimitiveArrayReader(primitiveType);
        yield nullCheckAndDelegate(primitiveArrayReader);
      }
      case TypeExpr2.RefValueNode(var refValueType, var javaType) -> {
        Class<?> clazz = (Class<?>) javaType;
        if (customReaders.containsKey(clazz)) {
          yield customReaders.get(clazz);
        } else {
          yield buildValueReader(refValueType, typeReaderResolver);
        }
      }
      case TypeExpr2.ArrayNode(var element, var componentType) -> {
        final Reader elementReader;
        if (element instanceof TypeExpr2.PrimitiveArrayNode(var primitiveType, var ignored2)) {
          // For nested primitive arrays, wrap the primitive array reader with null checking
          final var primitiveArrayReader = buildPrimitiveArrayReader(primitiveType);
          elementReader = nullCheckAndDelegate(primitiveArrayReader);
        } else {
          elementReader = createReaderChain(element, customReaders, customMarkers, typeReaderResolver);
        }
        final var nonNullArrayReader = createArrayReader(elementReader, componentType, element);
        yield nullCheckAndDelegate(nonNullArrayReader);
      }
      case TypeExpr2.ListNode(var element) -> {
        final var elementReader = createReaderChain(element, customReaders, customMarkers, typeReaderResolver);
        final var nonNullListReader = createListReader(elementReader);
        yield nullCheckAndDelegate(nonNullListReader);
      }
      case TypeExpr2.MapNode(var key, var value) -> {
        final var keyReader = createReaderChain(key, customReaders, customMarkers, typeReaderResolver);
        final var valueReader = createReaderChain(value, customReaders, customMarkers, typeReaderResolver);
        final var nonNullMapReader = createMapReader(keyReader, valueReader);
        yield nullCheckAndDelegate(nonNullMapReader);
      }
      case TypeExpr2.OptionalNode(var wrapped) -> {
        final var valueReader = createReaderChain(wrapped, customReaders, customMarkers, typeReaderResolver);
        final var nonNullOptionalReader = createOptionalReader(valueReader);
        yield nullCheckAndDelegate(nonNullOptionalReader);
      }
    };
  }

  static Writer createComponentWriter(TypeExpr2 typeExpr, MethodHandle getter, Map<Class<?>, Writer> customWriters, Map<Class<?>, Integer> customMarkers, WriterResolver typeWriterResolver) {
    LOGGER.fine(() -> "Creating component writer for: " + typeExpr.toTreeString());

    Writer writerChain = createWriterChain(typeExpr, customWriters, customMarkers, typeWriterResolver);

    return extractAndDelegateWriter(writerChain, getter);
  }

  /// Create writer chain for TypeExpr2 
  static Writer createWriterChain(TypeExpr2 typeExpr, Map<Class<?>, Writer> customWriters, Map<Class<?>, Integer> customMarkers, WriterResolver typeWriterResolver) {
    return switch (typeExpr) {
      case TypeExpr2.PrimitiveValueNode(var primitiveType, var ignored) -> buildPrimitiveValueWriter(primitiveType);
      case TypeExpr2.PrimitiveArrayNode(var primitiveType, var ignored) ->
          buildPrimitiveArrayWriterInner(primitiveType);
      case TypeExpr2.RefValueNode(var refValueType, var javaType) -> {
        Class<?> clazz = (Class<?>) javaType;
        if (customWriters.containsKey(clazz)) {
          yield customWriters.get(clazz);
        } else if (refValueType == TypeExpr2.RefValueType.RECORD || refValueType == TypeExpr2.RefValueType.INTERFACE || refValueType == TypeExpr2.RefValueType.ENUM) {
          yield typeWriterResolver.apply(clazz);
        } else {
          yield buildRefValueWriter(refValueType, javaType);
        }
      }
      case TypeExpr2.ArrayNode(var element, var ignored) -> {
        Writer elementWriter = createWriterChain(element, customWriters, customMarkers, typeWriterResolver);
        yield createArrayRefWriter(elementWriter, element);
      }
      case TypeExpr2.ListNode(var element) -> {
        Writer elementWriter = createWriterChain(element, customWriters, customMarkers, typeWriterResolver);
        yield createListWriterInner(elementWriter);
      }
      case TypeExpr2.MapNode(var key, var value) -> {
        Writer keyWriter = createWriterChain(key, customWriters, customMarkers, typeWriterResolver);
        Writer valueWriter = createWriterChain(value, customWriters, customMarkers, typeWriterResolver);
        yield createMapWriterInner(keyWriter, valueWriter);
      }
      case TypeExpr2.OptionalNode(var wrapped) -> {
        Writer valueWriter = createWriterChain(wrapped, customWriters, customMarkers, typeWriterResolver);
        yield createOptionalWriterInner(valueWriter);
      }
    };
  }

  /// Build ref value writer for TypeExpr2.RefValueType
  static @NotNull Writer buildRefValueWriter(TypeExpr2.RefValueType refValueType, Type javaType) {
    Class<?> cls = (Class<?>) javaType;
    return switch (refValueType) {
      case BOOLEAN -> (buffer, obj) -> buffer.put((byte) ((boolean) obj ? 1 : 0));
      case BYTE -> (buffer, obj) -> buffer.put((byte) obj);
      case SHORT -> (buffer, obj) -> buffer.putShort((short) obj);
      case CHARACTER -> (buffer, obj) -> buffer.putChar((char) obj);
      case INTEGER -> (ByteBuffer buffer, Object object) -> {
        final int result = (int) object;
        final var position = buffer.position();
        if (ZigZagEncoding.sizeOf(result) < Integer.BYTES) {
          ZigZagEncoding.putInt(buffer, Constants2.INTEGER_VAR.marker());
          LOGGER.fine(() -> "Writing INTEGER_VAR value=" + result + " at position: " + position);
          ZigZagEncoding.putInt(buffer, result);
        } else {
          ZigZagEncoding.putInt(buffer, Constants2.INTEGER.marker());
          LOGGER.fine(() -> "Writing INTEGER value=" + result + " at position: " + position);
          buffer.putInt(result);
        }
      };
      case LONG -> (ByteBuffer buffer, Object record) -> {
        final long result = (long) record;
        final var position = buffer.position();
        if (ZigZagEncoding.sizeOf(result) < Long.BYTES) {
          ZigZagEncoding.putInt(buffer, Constants2.LONG_VAR.marker());
          LOGGER.fine(() -> "Writing LONG_VAR value=" + result + " at position: " + position);
          ZigZagEncoding.putLong(buffer, result);
        } else {
          ZigZagEncoding.putInt(buffer, Constants2.LONG.marker());
          LOGGER.finer(() -> "Writing LONG value=" + result + " at position: " + position);
          buffer.putLong(result);
        }
      };
      case FLOAT -> (buffer, obj) -> buffer.putFloat((float) obj);
      case DOUBLE -> (buffer, obj) -> buffer.putDouble((double) obj);
      case STRING -> (buffer, obj) -> {
        final String str = (String) obj;
        final byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        ZigZagEncoding.putInt(buffer, bytes.length);
        buffer.put(bytes);
      };
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
      default -> throw new IllegalArgumentException("Unsupported ref value type: " + refValueType);
    };
  }

  /// Container type markers
  enum ContainerType {
    OPTIONAL_EMPTY(-16),
    OPTIONAL_OF(-17),
    ARRAY(-18),
    MAP(-19),
    LIST(-20);

    private final int marker;

    ContainerType(int marker) {
      this.marker = marker;
    }

    int marker() {
      return marker;
    }
  }

  /// Build primitive array writer for TypeExpr.PrimitiveValueType (old version)
  static @NotNull Writer buildPrimitiveArrayWriterInner(TypeExpr2.PrimitiveValueType primitiveType) {
    LOGGER.fine(() -> "Building writer chain for primitive array type: " + primitiveType);
    return switch (primitiveType) {
      case BOOLEAN -> (buffer, inner) -> {
        final var position = buffer.position();
        final var booleans = (boolean[]) inner;
        ZigZagEncoding.putInt(buffer, Constants2.BOOLEAN.marker());
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
        ZigZagEncoding.putInt(buffer, Constants2.BYTE.marker());
        ZigZagEncoding.putInt(buffer, bytes.length);
        buffer.put(bytes);
      };
      case SHORT -> (buffer, inner) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag SHORT at position " + buffer.position());
        ZigZagEncoding.putInt(buffer, Constants2.SHORT.marker());
        final var shorts = (short[]) inner;
        ZigZagEncoding.putInt(buffer, shorts.length);
        for (short s : shorts) {
          buffer.putShort(s);
        }
      };
      case CHARACTER -> (buffer, inner) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag CHARACTER at position " + buffer.position());
        final var chars = (char[]) inner;
        ZigZagEncoding.putInt(buffer, Constants2.CHARACTER.marker());
        ZigZagEncoding.putInt(buffer, chars.length);
        for (char c : chars) {
          buffer.putChar(c);
        }
      };
      case FLOAT -> (buffer, inner) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag FLOAT at position " + buffer.position());
        final var floats = (float[]) inner;
        ZigZagEncoding.putInt(buffer, Constants2.FLOAT.marker());
        ZigZagEncoding.putInt(buffer, floats.length);
        for (float f : floats) {
          buffer.putFloat(f);
        }
      };
      case DOUBLE -> (buffer, inner) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag DOUBLE at position " + buffer.position());
        ZigZagEncoding.putInt(buffer, Constants2.DOUBLE.marker());
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
          LOGGER.finer(() -> "Writing INTEGER_VAR marker " + Constants2.INTEGER_VAR.marker() + " at position " + buffer.position() + " length=" + length);
          ZigZagEncoding.putInt(buffer, Constants2.INTEGER_VAR.marker());
          ZigZagEncoding.putInt(buffer, length);
          IntStream.range(0, integers.length).forEach(i -> {
            LOGGER.finer(() -> "Writing INTEGER_VAR element " + i + " value=" + integers[i] + " at position " + buffer.position());
            ZigZagEncoding.putInt(buffer, integers[i]);
          });
        } else {
          LOGGER.finer(() -> "Writing INTEGER marker " + Constants2.INTEGER.marker() + " at position " + buffer.position() + " length=" + length);
          ZigZagEncoding.putInt(buffer, Constants2.INTEGER.marker());
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
          ZigZagEncoding.putInt(buffer, Constants2.LONG_VAR.marker());
          ZigZagEncoding.putInt(buffer, length);
          for (long i : longs) {
            ZigZagEncoding.putLong(buffer, i);
          }
        } else {
          LOGGER.fine(() -> "Writing LONG array - position=" + buffer.position() + " length=" + length);
          ZigZagEncoding.putInt(buffer, Constants2.LONG.marker());
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
  static @NotNull Writer buildPrimitiveValueWriter(TypeExpr2.PrimitiveValueType primitiveType) {
    return switch (primitiveType) {
      case BOOLEAN -> (ByteBuffer buffer, Object result) -> buffer.put((byte) ((boolean) result ? 1 : 0));
      case BYTE -> (ByteBuffer buffer, Object result) -> buffer.put((byte) result);
      case SHORT -> (ByteBuffer buffer, Object result) -> buffer.putShort((short) result);
      case CHARACTER -> (ByteBuffer buffer, Object result) -> buffer.putChar((char) result);
      case INTEGER -> (ByteBuffer buffer, Object object) -> {
        final int result = (int) object;
        final var position = buffer.position();
        if (ZigZagEncoding.sizeOf(result) < Integer.BYTES) {
          LOGGER.fine(() -> "Writing primitive INTEGER_VAR marker=" + Constants2.INTEGER_VAR.marker() + " value=" + result + " at position: " + position);
          ZigZagEncoding.putInt(buffer, Constants2.INTEGER_VAR.marker());
          ZigZagEncoding.putInt(buffer, result);
        } else {
          LOGGER.fine(() -> "Writing primitive marker=" + Constants2.INTEGER_VAR.marker() + "  INTEGER value=" + result + " at position: " + position);
          ZigZagEncoding.putInt(buffer, Constants2.INTEGER.marker());
          buffer.putInt(result);
        }
      };
      case LONG -> {
        LOGGER.fine(() -> "Building writer chain for long.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          final long result = (long) record;
          final var position = buffer.position();
          if (ZigZagEncoding.sizeOf(result) < Long.BYTES) {
            ZigZagEncoding.putInt(buffer, Constants2.LONG_VAR.marker());
            LOGGER.fine(() -> "Writing LONG_VAR value=" + result + " at position: " + position);
            ZigZagEncoding.putLong(buffer, result);
          } else {
            ZigZagEncoding.putInt(buffer, Constants2.LONG.marker());
            LOGGER.finer(() -> "Writing LONG value=" + result + " at position: " + position);
            buffer.putLong(result);
          }
        };
      }
      case FLOAT -> (ByteBuffer buffer, Object inner) -> buffer.putFloat((Float) inner);
      case DOUBLE -> (ByteBuffer buffer, Object inner) -> buffer.putDouble((Double) inner);
    };
  }

  static @NotNull Reader buildPrimitiveValueReader(TypeExpr2.PrimitiveValueType primitiveType) {
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
        if (marker == Constants2.INTEGER_VAR.marker()) {
          int value = ZigZagEncoding.getInt(buffer);
          LOGGER.finer(() -> "INTEGER reader: read INTEGER_VAR value=" + value + " at position=" + buffer.position());
          return value;
        } else if (marker == Constants2.INTEGER.marker()) {
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
        if (marker == Constants2.LONG_VAR.marker()) {
          LOGGER.finer(() -> "LONG reader: read LONG_VAR marker=" + marker + " at position=" + position);
          return ZigZagEncoding.getLong(buffer);
        } else if (marker == Constants2.LONG.marker()) {
          LOGGER.finer(() -> "LONG reader: read LONG marker=" + marker + " at position=" + position);
          return buffer.getLong();
        } else {
          throw new IllegalStateException("Expected LONG or LONG_VAR marker but got: " + marker + " at position: " + position);
        }
      };
    };
  }

  static @NotNull Reader buildPrimitiveArrayReader(TypeExpr2.PrimitiveValueType primitiveType) {
    return switch (primitiveType) {
      case BOOLEAN -> (buffer) -> {
        int marker = ZigZagEncoding.getInt(buffer);
        assert marker == Constants2.BOOLEAN.marker() : "Expected BOOLEAN marker but got: " + marker;
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
        assert marker == Constants2.BYTE.marker() : "Expected BYTE marker but got: " + marker;
        int length = ZigZagEncoding.getInt(buffer);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
      };
      case SHORT -> (buffer) -> {
        int marker = ZigZagEncoding.getInt(buffer);
        assert marker == Constants2.SHORT.marker() : "Expected SHORT marker but got: " + marker;
        int length = ZigZagEncoding.getInt(buffer);
        short[] shorts = new short[length];
        IntStream.range(0, length).forEach(i -> shorts[i] = buffer.getShort());
        return shorts;
      };
      case CHARACTER -> (buffer) -> {
        int marker = ZigZagEncoding.getInt(buffer);
        assert marker == Constants2.CHARACTER.marker() : "Expected CHARACTER marker but got: " + marker;
        int length = ZigZagEncoding.getInt(buffer);
        char[] chars = new char[length];
        IntStream.range(0, length).forEach(i -> chars[i] = buffer.getChar());
        return chars;
      };
      case FLOAT -> (buffer) -> {
        int marker = ZigZagEncoding.getInt(buffer);
        assert marker == Constants2.FLOAT.marker() : "Expected FLOAT marker but got: " + marker;
        int length = ZigZagEncoding.getInt(buffer);
        float[] floats = new float[length];
        IntStream.range(0, length).forEach(i -> floats[i] = buffer.getFloat());
        return floats;
      };
      case DOUBLE -> (buffer) -> {
        int marker = ZigZagEncoding.getInt(buffer);
        assert marker == Constants2.DOUBLE.marker() : "Expected DOUBLE marker but got: " + marker;
        int length = ZigZagEncoding.getInt(buffer);
        double[] doubles = new double[length];
        IntStream.range(0, length).forEach(i -> doubles[i] = buffer.getDouble());
        return doubles;
      };
      case INTEGER -> (buffer) -> {
        int positionBeforeMarker = buffer.position();
        int marker = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "Reading INTEGER array marker " + marker + " at position " + positionBeforeMarker);
        if (marker == Constants2.INTEGER_VAR.marker()) {
          int length = ZigZagEncoding.getInt(buffer);
          LOGGER.finer(() -> "Reading INTEGER_VAR array with length " + length + " at position " + buffer.position());
          int[] integers = new int[length];
          IntStream.range(0, length).forEach(i -> {
            int positionBeforeElement = buffer.position();
            integers[i] = ZigZagEncoding.getInt(buffer);
            LOGGER.finer(() -> "Read INTEGER_VAR element " + i + " value=" + integers[i] + " from position " + positionBeforeElement);
          });
          return integers;
        } else if (marker == Constants2.INTEGER.marker()) {
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
        if (marker == Constants2.LONG_VAR.marker()) {
          int length = ZigZagEncoding.getInt(buffer);
          long[] longs = new long[length];
          IntStream.range(0, length).forEach(i -> longs[i] = ZigZagEncoding.getLong(buffer));
          return longs;
        } else if (marker == Constants2.LONG.marker()) {
          int length = ZigZagEncoding.getInt(buffer);
          long[] longs = new long[length];
          IntStream.range(0, length).forEach(i -> longs[i] = buffer.getLong());
          return longs;
        } else throw new IllegalStateException("Expected LONG or LONG_VAR marker but got: " + marker);
      };
    };
  }

  static @NotNull Sizer buildPrimitiveArraySizer(TypeExpr2.PrimitiveValueType primitiveType) {
    if (primitiveType == BOOLEAN) {
      return (Object value) -> 2 * Integer.BYTES + Array.getLength(value) * Byte.BYTES;
    } else if (primitiveType == BYTE) {
      return (Object value) -> 2 * Integer.BYTES + Array.getLength(value) * Byte.BYTES;
    } else if (primitiveType == SHORT) {
      return (Object value) -> 2 * Integer.BYTES + Array.getLength(value) * Short.BYTES;
    } else if (primitiveType == CHARACTER) {
      return (Object value) -> 2 * Integer.BYTES + Array.getLength(value) * Character.BYTES;
    } else if (primitiveType == TypeExpr2.PrimitiveValueType.INTEGER) {
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

  static @NotNull Sizer buildValueSizer(TypeExpr2.RefValueType refValueType, Type javaType, SizerResolver typeSizerResolver) {
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

  static @NotNull Sizer buildPrimitiveValueSizer(TypeExpr2.PrimitiveValueType primitiveType) {
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

  static @NotNull Sizer buildPrimitiveArraySizerInner(TypeExpr2.PrimitiveValueType primitiveType) {
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

  static Class<?> typeExprToClass(TypeExpr2 typeExpr) {
    return switch (typeExpr) {
      case TypeExpr2.ArrayNode(var element, var ignored) -> {
        Class<?> componentClass = typeExprToClass(element);
        yield Array.newInstance(componentClass, 0).getClass();
      }
      case TypeExpr2.RefValueNode(var ignored, var javaType) -> (Class<?>) javaType;
      case TypeExpr2.PrimitiveValueNode(var ignored, var javaType) -> (Class<?>) javaType;
      case TypeExpr2.ListNode ignored -> List.class;
      case TypeExpr2.MapNode ignored -> Map.class;
      case TypeExpr2.OptionalNode ignored -> Optional.class;
      case TypeExpr2.PrimitiveArrayNode(var ignored, var javaType) -> (Class<?>) javaType;
      default -> throw new IllegalStateException("Unexpected value: " + typeExpr);
    };
  }

  static Sizer extractAndDelegate(Sizer delegate, MethodHandle accessor) {
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

  static Writer extractAndDelegate(Writer delegate, MethodHandle
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
  static Writer extractAndDelegateWriter(Writer delegate, MethodHandle accessor) {
    return extractAndDelegate(delegate, accessor);
  }

  /// Build sizer chain with callback for complex types
  static Sizer buildSizerChain(TypeExpr2 typeExpr, MethodHandle accessor,
                               SizerResolver complexResolver) {
    return extractAndDelegate(buildSizerChainInner(typeExpr, complexResolver), accessor);
  }

  /// Build sizer chain inner with callback delegation
  static Sizer buildSizerChainInner(TypeExpr2 typeExpr,
                                    SizerResolver complexResolver) {
    return switch (typeExpr) {
      case TypeExpr2.PrimitiveValueNode(var primitiveType, var ignored) -> buildPrimitiveValueSizer(primitiveType);
      case TypeExpr2.RefValueNode(var refValueType, var javaType) ->
          buildValueSizerInner(refValueType, javaType, complexResolver);
      case TypeExpr2.ArrayNode(var element, var ignored) -> {
        if (element instanceof TypeExpr2.PrimitiveValueNode(var primitiveType, var ignored2)) {
          yield buildPrimitiveArraySizerInner(primitiveType);
        } else {
          final var elementSizer = buildSizerChainInner(element, complexResolver);
          yield createArraySizerInner(elementSizer);
        }
      }
      case TypeExpr2.ListNode(var element) -> {
        final var elementSizer = buildSizerChainInner(element, complexResolver);
        yield createListSizerInner(elementSizer);
      }
      case TypeExpr2.MapNode(var key, var value) -> {
        final var keySizer = buildSizerChainInner(key, complexResolver);
        final var valueSizer = buildSizerChainInner(value, complexResolver);
        yield createMapSizerInner(keySizer, valueSizer);
      }
      case TypeExpr2.OptionalNode(var wrapped) -> {
        final var valueSizer = buildSizerChainInner(wrapped, complexResolver);
        yield createOptionalSizerInner(valueSizer);
      }
      default -> throw new IllegalStateException("Unexpected value: " + typeExpr);
    };
  }

  /// Build value sizer with callback for complex types (RECORD, INTERFACE, ENUM)
  static Sizer buildValueSizerInner(TypeExpr2.RefValueType refValueType, Type javaType,
                                    SizerResolver complexResolver) {
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
  static Writer buildWriterChain(TypeExpr2 typeExpr, MethodHandle accessor,
                                 WriterResolver complexResolver) {
    return extractAndDelegate(buildWriterChainInner(typeExpr, complexResolver), accessor);
  }

  /// Build writer chain inner with callback delegation
  static Writer buildWriterChainInner(TypeExpr2 typeExpr,
                                      WriterResolver complexResolver) {
    return switch (typeExpr) {
      case TypeExpr2.PrimitiveValueNode(var primitiveType, var ignored) -> buildPrimitiveValueWriter(primitiveType);
      case TypeExpr2.RefValueNode(var refValueType, var javaType) ->
          buildValueWriter(refValueType, javaType, complexResolver);
      case TypeExpr2.ArrayNode(var element, var ignored) -> {
        if (element instanceof TypeExpr2.PrimitiveValueNode(var primitiveType, var ignored2)) {
          yield buildPrimitiveArrayWriterInner(primitiveType);
        } else {
          final var elementWriter = buildWriterChainInner(element, complexResolver);
          yield createArrayRefWriter(elementWriter, element);
        }
      }
      case TypeExpr2.ListNode(var element) -> {
        final var elementWriter = buildWriterChainInner(element, complexResolver);
        yield createListWriterInner(elementWriter);
      }
      case TypeExpr2.MapNode(var key, var value) -> {
        final var keyWriter = buildWriterChainInner(key, complexResolver);
        final var valueWriter = buildWriterChainInner(value, complexResolver);
        yield createMapWriterInner(keyWriter, valueWriter);
      }
      case TypeExpr2.OptionalNode(var wrapped) -> {
        final var valueWriter = buildWriterChainInner(wrapped, complexResolver);
        yield createOptionalWriterInner(valueWriter);
      }
      default -> throw new IllegalStateException("Unexpected value: " + typeExpr);
    };
  }

  /// Build value writer with callback for complex types
  static Writer buildValueWriter(TypeExpr2.RefValueType refValueType, Type javaType,
                                 WriterResolver complexResolver) {
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
            ZigZagEncoding.putInt(buffer, Constants2.INTEGER_VAR.marker());
            LOGGER.fine(() -> "Writing INTEGER_VAR value=" + result + " at position: " + position);
            ZigZagEncoding.putInt(buffer, result);
          } else {
            ZigZagEncoding.putInt(buffer, Constants2.INTEGER.marker());
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
              ZigZagEncoding.putInt(buffer, Constants2.LONG_VAR.marker());
              LOGGER.fine(() -> "Writing LONG_VAR value=" + result + " at position: " + position);
              ZigZagEncoding.putLong(buffer, result);
            } else {
              ZigZagEncoding.putInt(buffer, Constants2.LONG.marker());
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
  static Reader buildReaderChain(TypeExpr2 typeExpr,
                                 ReaderResolver complexResolver) {
    return switch (typeExpr) {
      case TypeExpr2.PrimitiveValueNode(var primitiveType, var ignored) -> buildPrimitiveValueReader(primitiveType);
      case TypeExpr2.RefValueNode(var refValueType, var ignored) -> buildValueReader(refValueType, complexResolver);
      case TypeExpr2.PrimitiveArrayNode(var primitiveType, var ignored) -> buildPrimitiveArrayReader(primitiveType);
      case TypeExpr2.ArrayNode(var element, var ignored3) -> {
        final Reader nonNullArrayReader;
        if (element instanceof TypeExpr2.PrimitiveValueNode(var primitiveType, var ignored2)) {
          nonNullArrayReader = buildPrimitiveArrayReader(primitiveType);
        } else if (element instanceof TypeExpr2.PrimitiveArrayNode(var primitiveType, var ignored2)) {
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
      case TypeExpr2.ListNode(var element) -> {
        final var elementReader = buildReaderChain(element, complexResolver);
        final var nonNullListReader = createListReader(elementReader);
        yield nullCheckAndDelegate(nonNullListReader);
      }
      case TypeExpr2.MapNode(var key, var value) -> {
        final var keyReader = buildReaderChain(key, complexResolver);
        final var valueReader = buildReaderChain(value, complexResolver);
        final var nonNullMapReader = createMapReader(keyReader, valueReader);
        yield nullCheckAndDelegate(nonNullMapReader);
      }
      case TypeExpr2.OptionalNode(var wrapped) -> {
        final var valueReader = buildReaderChain(wrapped, complexResolver);
        final var nonNullOptionalReader = createOptionalReader(valueReader);
        yield nullCheckAndDelegate(nonNullOptionalReader);
      }
      default -> throw new IllegalStateException("Unexpected value: " + typeExpr);
    };
  }

  /// Build value reader with callback for complex types
  static Reader buildValueReader(TypeExpr2.RefValueType refValueType,
                                 ReaderResolver complexResolver) {
    final Reader primitiveReader = buildRefValueReader(refValueType, complexResolver);
    return nullCheckAndDelegate(primitiveReader);
  }

  /// Build ref value reader with callback for complex types
  static Reader buildRefValueReader(TypeExpr2.RefValueType refValueType,
                                    ReaderResolver complexResolver) {
    return switch (refValueType) {
      case BOOLEAN -> buffer -> buffer.get() != 0;
      case BYTE -> ByteBuffer::get;
      case SHORT -> ByteBuffer::getShort;
      case CHARACTER -> ByteBuffer::getChar;
      case INTEGER -> buffer -> {
        final var positionBefore = buffer.position();
        LOGGER.fine(() -> "RefValueReader reading INTEGER_VAR or INTEGER marker at position: " + positionBefore);
        final int marker = ZigZagEncoding.getInt(buffer);
        if (marker == Constants2.INTEGER_VAR.marker()) {
          return ZigZagEncoding.getInt(buffer);
        } else if (marker == Constants2.INTEGER.marker()) {
          return buffer.getInt();
        } else {
          throw new IllegalStateException("Expected INTEGER marker but got: " + marker + " at position: " + positionBefore);
        }
      };
      case LONG -> buffer -> {
        final var positionBefore = buffer.position();
        LOGGER.fine(() -> "RefValueReader reading LONG_VAR or LONG marker at position: " + positionBefore);
        final int marker = ZigZagEncoding.getInt(buffer);
        if (marker == Constants2.LONG_VAR.marker()) {
          return ZigZagEncoding.getLong(buffer);
        } else if (marker == Constants2.LONG.marker()) {
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
  static Reader nullCheckAndDelegate(Reader delegate) {
    return buffer -> {
      final int positionBefore = buffer.position();
      final byte nullMarker = buffer.get();
      LOGGER.finer(() -> "Read null marker " + nullMarker + " at position " + positionBefore +
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
  static Map<Class<?>, Long> computeRecordTypeSignatures(List<Class<?>> recordClasses) {
    return recordClasses.stream()
        .filter(Class::isRecord) // Only process actual record classes
        .collect(Collectors.toMap(
            clz -> clz,
            clz -> {
              final var components = clz.getRecordComponents();
              final var typeExprs = Arrays.stream(components)
                  .map(comp -> TypeExpr2.analyzeType(comp.getGenericType(), Set.of()))
                  .toArray(TypeExpr2[]::new);
              final var signature = hashClassSignature(clz, components, typeExprs);
              LOGGER.finer(() -> "Computed type signature for " + clz.getName() + ": 0x" + Long.toHexString(signature));
              return signature;
            }
        ));
  }

  /// Compute a type signature from full class name, the component types, and component name
  static long hashClassSignature(Class<?> clazz, RecordComponent[] components, TypeExpr2[] componentTypes) {
    String input = Stream.concat(Stream.of(clazz.getName()),
            IntStream.range(0, components.length).boxed().flatMap(i ->
                Stream.concat(Stream.of(componentTypes[i].toTreeString()), Stream.of(components[i].getName()))))
        .collect(Collectors.joining("!"));
    final var signature = hashSignature(input);
    LOGGER.fine(() -> "Signature for " + clazz.getName() + " input '" + input + "' -> 0x" + Long.toHexString(signature));
    return signature;
  }

  /// Compute a type signature from enum class full name and constant names
  static long hashEnumSignature(Class<?> enumClass) {
    Object[] enumConstants = enumClass.getEnumConstants();
    assert enumConstants != null : "Not an enum class: " + enumClass;
    String input = Stream.concat(Stream.of(enumClass.getName()), Arrays.stream(enumConstants).map(e -> ((Enum<?>) e).name())).collect(Collectors.joining("!"));
    return hashSignature(input);
  }

  /// This method computes a 64 bit signature from a unique string representation by hashing it using SHA-256
  /// then extracting the first `Long.BYTES` big endian bytes into a long.
  /// INTERNAL USE ONLY - Use hashClassSignature() or hashEnumSignature() instead
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
    result = IntStream.range(0, Long.BYTES).mapToLong(i -> (hash[i] & -FFL) << (56 - i * 8)).reduce(0L, (a, b) -> a | b);
    return result;
  }

  /// Create lazy sizer that uses callback for record types
  static Sizer createLazySizer(TypeExpr2 typeExpr, MethodHandle getter, DependencyResolver resolver) {
    final Sizer sizerChain = createLazySizerChain(typeExpr, resolver);
    return extractAndDelegate(sizerChain, getter);
  }

  /// Create lazy writer that uses callback for record types
  static Writer createLazyWriter(TypeExpr2 typeExpr, MethodHandle getter, DependencyResolver resolver) {
    final Writer writerChain = createLazyWriterChain(typeExpr, resolver);
    return extractAndDelegateWriter(writerChain, getter);
  }

  /// Create lazy reader that uses callback for record types
  static Reader createLazyReader(TypeExpr2 typeExpr, DependencyResolver resolver) {
    return createLazyReaderChain(typeExpr, resolver);
  }

  /// Create lazy sizer chain with callback delegation
  static Sizer createLazySizerChain(TypeExpr2 typeExpr, DependencyResolver resolver) {
    return switch (typeExpr) {
      case TypeExpr2.PrimitiveValueNode(var primitiveType, var ignored) -> buildPrimitiveValueSizer(primitiveType);
      case TypeExpr2.PrimitiveArrayNode(var primitiveType, var ignored) -> buildPrimitiveArraySizer(primitiveType);
      case TypeExpr2.RefValueNode(var refValueType, var javaType) -> {
        Class<?> clazz = (Class<?>) javaType;
        if (clazz.isRecord()) {
          yield obj -> {
            if (obj == null) return Byte.BYTES;
            @SuppressWarnings("unchecked")
            final var pickler = (Pickler<Object>) resolver.resolve(clazz);
            return Byte.BYTES + pickler.maxSizeOf(obj);
          };
        } else if (clazz.isEnum()) {
          yield obj -> obj == null ? Byte.BYTES : Byte.BYTES + Integer.BYTES;
        } else if (clazz.isInterface()) {
          yield obj -> {
            if (obj == null) return Byte.BYTES;
            @SuppressWarnings("unchecked")
            final var pickler = (Pickler<Object>) resolver.resolve(obj.getClass());
            return Byte.BYTES + pickler.maxSizeOf(obj);
          };
        } else {
          yield buildValueSizerInner(refValueType, javaType, cls -> {
            @SuppressWarnings("unchecked")
            final var pickler = (Pickler<Object>) resolver.resolve(cls);
            return pickler::maxSizeOf;
          });
        }
      }
      case TypeExpr2.ArrayNode(var element, var ignored) -> {
        final var elementSizer = createLazySizerChain(element, resolver);
        yield createArraySizerInner(elementSizer);
      }
      case TypeExpr2.ListNode(var element) -> {
        final var elementSizer = createLazySizerChain(element, resolver);
        yield createListSizerInner(elementSizer);
      }
      case TypeExpr2.MapNode(var key, var value) -> {
        final var keySizer = createLazySizerChain(key, resolver);
        final var valueSizer = createLazySizerChain(value, resolver);
        yield createMapSizerInner(keySizer, valueSizer);
      }
      case TypeExpr2.OptionalNode(var wrapped) -> {
        final var valueSizer = createLazySizerChain(wrapped, resolver);
        yield createOptionalSizerInner(valueSizer);
      }
    };
  }

  /// Create lazy writer chain with callback delegation
  static Writer createLazyWriterChain(TypeExpr2 typeExpr, DependencyResolver resolver) {
    return switch (typeExpr) {
      case TypeExpr2.PrimitiveValueNode(var primitiveType, var ignored) -> buildPrimitiveValueWriter(primitiveType);
      case TypeExpr2.PrimitiveArrayNode(var primitiveType, var ignored) -> buildPrimitiveArrayWriterInner(primitiveType);
      case TypeExpr2.RefValueNode(var refValueType, var javaType) -> {
        Class<?> clazz = (Class<?>) javaType;
        if (clazz.isRecord()) {
          yield (buffer, obj) -> {
            @SuppressWarnings("unchecked")
            final var pickler = (Pickler<Object>) resolver.resolve(clazz);
            pickler.serialize(buffer, obj);
          };
        } else if (clazz.isEnum()) {
          yield (buffer, obj) -> {
            final Enum<?> enumValue = (Enum<?>) obj;
            ZigZagEncoding.putInt(buffer, enumValue.ordinal());
          };
        } else if (clazz.isInterface()) {
          yield (buffer, obj) -> {
            @SuppressWarnings("unchecked")
            final var pickler = (Pickler<Object>) resolver.resolve(obj.getClass());
            pickler.serialize(buffer, obj);
          };
        } else {
          yield buildValueWriter(refValueType, javaType, cls -> {
            @SuppressWarnings("unchecked")
            final var pickler = (Pickler<Object>) resolver.resolve(cls);
            return pickler::serialize;
          });
        }
      }
      case TypeExpr2.ArrayNode(var element, var ignored) -> {
        final var elementWriter = createLazyWriterChain(element, resolver);
        yield createArrayRefWriter(elementWriter, element);
      }
      case TypeExpr2.ListNode(var element) -> {
        final var elementWriter = createLazyWriterChain(element, resolver);
        yield createListWriterInner(elementWriter);
      }
      case TypeExpr2.MapNode(var key, var value) -> {
        final var keyWriter = createLazyWriterChain(key, resolver);
        final var valueWriter = createLazyWriterChain(value, resolver);
        yield createMapWriterInner(keyWriter, valueWriter);
      }
      case TypeExpr2.OptionalNode(var wrapped) -> {
        final var valueWriter = createLazyWriterChain(wrapped, resolver);
        yield createOptionalWriterInner(valueWriter);
      }
    };
  }

  /// Create lazy reader chain with callback delegation
  static Reader createLazyReaderChain(TypeExpr2 typeExpr, DependencyResolver resolver) {
    return switch (typeExpr) {
      case TypeExpr2.PrimitiveValueNode(var primitiveType, var ignored) -> buildPrimitiveValueReader(primitiveType);
      case TypeExpr2.PrimitiveArrayNode(var primitiveType, var ignored) -> {
        final var primitiveArrayReader = buildPrimitiveArrayReader(primitiveType);
        yield nullCheckAndDelegate(primitiveArrayReader);
      }
      case TypeExpr2.RefValueNode(var refValueType, var javaType) -> {
        Class<?> clazz = (Class<?>) javaType;
        if (clazz.isRecord()) {
          final Reader recordReader = buffer -> {
            final long typeSignature = buffer.getLong();
            final var pickler = resolver.resolve(clazz);
            if (pickler instanceof RecordSerde<?> recordSerde) {
              return recordSerde.deserializeWithoutSignature(buffer);
            } else if (pickler instanceof EmptyRecordSerde<?> emptyRecordSerde) {
              return emptyRecordSerde.deserializeWithoutSignature(buffer);
            } else {
              throw new IllegalStateException("Unsupported serde type: " + pickler.getClass());
            }
          };
          yield nullCheckAndDelegate(recordReader);
        } else if (clazz.isEnum()) {
          final Reader enumReader = buffer -> {
            final int ordinal = ZigZagEncoding.getInt(buffer);
            final Object[] enumConstants = clazz.getEnumConstants();
            return enumConstants[ordinal];
          };
          yield nullCheckAndDelegate(enumReader);
        } else if (clazz.isInterface()) {
          final Reader interfaceReader = buffer -> {
            final long typeSignature = buffer.getLong();
            // For interfaces, we need to resolve the concrete type at runtime
            throw new UnsupportedOperationException("Interface type signature resolution not yet implemented");
          };
          yield nullCheckAndDelegate(interfaceReader);
        } else {
          yield buildValueReader(refValueType, typeSignature -> {
            // For non-record types, use the existing buildRefValueReader approach
            throw new UnsupportedOperationException("Type signature resolution not supported for non-record types");
          });
        }
      }
      case TypeExpr2.ArrayNode(var element, var componentType) -> {
        final Reader elementReader;
        if (element instanceof TypeExpr2.PrimitiveArrayNode(var primitiveType, var ignored2)) {
          final var primitiveArrayReader = buildPrimitiveArrayReader(primitiveType);
          elementReader = nullCheckAndDelegate(primitiveArrayReader);
        } else {
          elementReader = createLazyReaderChain(element, resolver);
        }
        final var nonNullArrayReader = createArrayReader(elementReader, componentType, element);
        yield nullCheckAndDelegate(nonNullArrayReader);
      }
      case TypeExpr2.ListNode(var element) -> {
        final var elementReader = createLazyReaderChain(element, resolver);
        final var nonNullListReader = createListReader(elementReader);
        yield nullCheckAndDelegate(nonNullListReader);
      }
      case TypeExpr2.MapNode(var key, var value) -> {
        final var keyReader = createLazyReaderChain(key, resolver);
        final var valueReader = createLazyReaderChain(value, resolver);
        final var nonNullMapReader = createMapReader(keyReader, valueReader);
        yield nullCheckAndDelegate(nonNullMapReader);
      }
      case TypeExpr2.OptionalNode(var wrapped) -> {
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
