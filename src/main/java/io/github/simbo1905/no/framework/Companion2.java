// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;

/// Companion for TypeExpr2-based component serde building
@SuppressWarnings("auxiliaryclass")
sealed interface Companion2 permits Companion2.Nothing {

  /// Build ComponentSerde array for a record type using TypeExpr2 AST
  @SuppressWarnings("auxiliaryclass")
  static ComponentSerde[] buildComponentSerdes(
      Class<?> recordClass,
      Collection<SerdeHandler> customHandlers,
      Function<Class<?>, ToIntFunction<Object>> typeSizerResolver,
      Function<Class<?>, BiConsumer<ByteBuffer, Object>> typeWriterResolver,
      Function<Class<?>, Function<ByteBuffer, Object>> typeReaderResolver
  ) {
    LOGGER.fine(() -> "Building ComponentSerde[] for record: " + recordClass.getName());

    RecordComponent[] components = recordClass.getRecordComponents();
    MethodHandle[] getters = resolveGetters(recordClass, components);

    return IntStream.range(0, components.length)
        .mapToObj(i -> {
          RecordComponent component = components[i];
          MethodHandle getter = getters[i];
          TypeExpr2 typeExpr = TypeExpr2.analyzeType(component.getGenericType(), customHandlers);

          LOGGER.finer(() -> "Component " + i + " (" + component.getName() + "): " + typeExpr.toTreeString());

          BiConsumer<ByteBuffer, Object> writer = createComponentWriter(typeExpr, getter, customHandlers, typeWriterResolver);
          Function<ByteBuffer, Object> reader = createComponentReader(typeExpr, customHandlers, typeReaderResolver);
          ToIntFunction<Object> sizer = createComponentSizer(typeExpr, getter, customHandlers, typeSizerResolver);

          return new ComponentSerde(writer, reader, sizer);
        })
        .toArray(ComponentSerde[]::new);
  }

  /// Create writer for a component based on its TypeExpr2
  static BiConsumer<ByteBuffer, Object> createComponentWriter(
      TypeExpr2 typeExpr,
      MethodHandle getter,
      @SuppressWarnings("auxiliaryclass") Collection<SerdeHandler> customHandlers,
      Function<Class<?>, BiConsumer<ByteBuffer, Object>> typeWriterResolver
  ) {
    return switch (typeExpr) {
      case TypeExpr2.PrimitiveValueNode(var type, var javaType, var marker) -> createPrimitiveWriter(type, getter);

      case TypeExpr2.RefValueNode(var refType, var javaType, var marker) -> {
        if (refType == TypeExpr2.RefValueType.CUSTOM) {
          // Find the custom handler
          SerdeHandler handler = customHandlers.stream()
              .filter(h -> h.valueBasedLike().equals(javaType))
              .findFirst()
              .orElseThrow(() -> new IllegalStateException("No handler for custom type: " + javaType));
          yield createCustomWriter(getter, handler);
        } else {
          yield createRefValueWriter(refType, getter, (Class<?>) javaType, typeWriterResolver);
        }
      }

      case TypeExpr2.ArrayNode ignored ->
          throw new UnsupportedOperationException("Array component writers not yet implemented");
      case TypeExpr2.ListNode ignored ->
          throw new UnsupportedOperationException("List component writers not yet implemented");
      case TypeExpr2.OptionalNode ignored ->
          throw new UnsupportedOperationException("Optional component writers not yet implemented");
      case TypeExpr2.MapNode ignored ->
          throw new UnsupportedOperationException("Map component writers not yet implemented");
    };
  }

  /// Create reader for a component based on its TypeExpr2
  static Function<ByteBuffer, Object> createComponentReader(
      TypeExpr2 typeExpr,
      @SuppressWarnings("auxiliaryclass") Collection<SerdeHandler> customHandlers,
      Function<Class<?>, Function<ByteBuffer, Object>> typeReaderResolver
  ) {
    return switch (typeExpr) {
      case TypeExpr2.PrimitiveValueNode(var type, var javaType, var marker) -> createPrimitiveReader(type);

      case TypeExpr2.RefValueNode(var refType, var javaType, var marker) -> {
        if (refType == TypeExpr2.RefValueType.CUSTOM) {
          SerdeHandler handler = customHandlers.stream()
              .filter(h -> h.valueBasedLike().equals(javaType))
              .findFirst()
              .orElseThrow(() -> new IllegalStateException("No handler for custom type: " + javaType));
          // Wrap custom reader with null handling
          yield buffer -> {
            byte nullMarker = buffer.get();
            if (nullMarker == NULL_MARKER) {
              return null;
            } else if (nullMarker == NOT_NULL_MARKER) {
              int typeMarker = ZigZagEncoding.getInt(buffer);
              assert typeMarker == handler.marker() : "Expected marker " + handler.marker() + " but got " + typeMarker;
              return handler.reader().apply(buffer);
            } else {
              throw new IllegalStateException("Invalid null marker: " + nullMarker);
            }
          };
        } else {
          yield createRefValueReader(refType, (Class<?>) javaType, typeReaderResolver);
        }
      }

      case TypeExpr2.ArrayNode ignored ->
          throw new UnsupportedOperationException("Array component readers not yet implemented");
      case TypeExpr2.ListNode ignored ->
          throw new UnsupportedOperationException("List component readers not yet implemented");
      case TypeExpr2.OptionalNode ignored ->
          throw new UnsupportedOperationException("Optional component readers not yet implemented");
      case TypeExpr2.MapNode ignored ->
          throw new UnsupportedOperationException("Map component readers not yet implemented");
    };
  }

  /// Create sizer for a component based on its TypeExpr2
  static ToIntFunction<Object> createComponentSizer(
      TypeExpr2 typeExpr,
      MethodHandle getter,
      @SuppressWarnings("auxiliaryclass") Collection<SerdeHandler> customHandlers,
      Function<Class<?>, ToIntFunction<Object>> typeSizerResolver
  ) {
    return switch (typeExpr) {
      case TypeExpr2.PrimitiveValueNode(var type, var javaType, var marker) -> createPrimitiveSizer(type);

      case TypeExpr2.RefValueNode(var refType, var javaType, var marker) -> {
        if (refType == TypeExpr2.RefValueType.CUSTOM) {
          SerdeHandler handler = customHandlers.stream()
              .filter(h -> h.valueBasedLike().equals(javaType))
              .findFirst()
              .orElseThrow(() -> new IllegalStateException("No handler for custom type: " + javaType));
          yield createCustomSizer(getter, handler);
        } else {
          yield createRefValueSizer(refType, getter, (Class<?>) javaType, typeSizerResolver);
        }
      }

      case TypeExpr2.ArrayNode ignored ->
          throw new UnsupportedOperationException("Array component sizers not yet implemented");
      case TypeExpr2.ListNode ignored ->
          throw new UnsupportedOperationException("List component sizers not yet implemented");
      case TypeExpr2.OptionalNode ignored ->
          throw new UnsupportedOperationException("Optional component sizers not yet implemented");
      case TypeExpr2.MapNode ignored ->
          throw new UnsupportedOperationException("Map component sizers not yet implemented");
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

  /// Create writer for primitive types
  static BiConsumer<ByteBuffer, Object> createPrimitiveWriter(TypeExpr2.PrimitiveValueType type, MethodHandle getter) {
    return (buffer, obj) -> {
      try {
        switch (type) {
          case BOOLEAN -> buffer.put((byte) ((boolean) getter.invoke(obj) ? 1 : 0));
          case BYTE -> buffer.put((byte) getter.invoke(obj));
          case SHORT -> buffer.putShort((short) getter.invoke(obj));
          case CHARACTER -> buffer.putChar((char) getter.invoke(obj));
          case INTEGER -> ZigZagEncoding.putInt(buffer, (int) getter.invoke(obj));
          case LONG -> ZigZagEncoding.putLong(buffer, (long) getter.invoke(obj));
          case FLOAT -> buffer.putFloat((float) getter.invoke(obj));
          case DOUBLE -> buffer.putDouble((double) getter.invoke(obj));
        }
      } catch (Throwable e) {
        throw new IllegalStateException("Failed to write primitive", e);
      }
    };
  }

  /// Create reader for primitive types
  static Function<ByteBuffer, Object> createPrimitiveReader(TypeExpr2.PrimitiveValueType type) {
    return buffer -> switch (type) {
      case BOOLEAN -> buffer.get() != 0;
      case BYTE -> buffer.get();
      case SHORT -> buffer.getShort();
      case CHARACTER -> buffer.getChar();
      case INTEGER -> ZigZagEncoding.getInt(buffer);
      case LONG -> ZigZagEncoding.getLong(buffer);
      case FLOAT -> buffer.getFloat();
      case DOUBLE -> buffer.getDouble();
    };
  }

  /// Create sizer for primitive types
  static ToIntFunction<Object> createPrimitiveSizer(TypeExpr2.PrimitiveValueType type) {
    // Primitive types don't need the record object, just return fixed sizes
    return obj -> switch (type) {
      case BOOLEAN, BYTE -> Byte.BYTES;
      case SHORT, CHARACTER -> Short.BYTES;
      case INTEGER -> Integer.BYTES; // For primitive int, we know it's fixed size
      case LONG -> Long.BYTES; // For primitive long, we know it's fixed size
      case FLOAT -> Float.BYTES;
      case DOUBLE -> Double.BYTES;
    };
  }

  /// Constants for null markers
  static final byte NULL_MARKER = Byte.MIN_VALUE;
  static final byte NOT_NULL_MARKER = Byte.MAX_VALUE;

  /// Extract value from record using getter and delegate to writer with null handling
  static BiConsumer<ByteBuffer, Object> extractAndDelegate(BiConsumer<ByteBuffer, Object> delegate, MethodHandle accessor) {
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
          buffer.put(NOT_NULL_MARKER); // write a marker for not null
          delegate.accept(buffer, value);
        }
      };
    }
  }

  /// Extract value from record using getter and delegate to sizer with null handling
  static ToIntFunction<Object> extractAndDelegate(ToIntFunction<Object> delegate, MethodHandle accessor) {
    return (Object record) -> {
      final Object value;
      try {
        value = accessor.invokeWithArguments(record);
      } catch (Throwable e) {
        throw new RuntimeException(e.getMessage(), e);
      }
      if (value == null) {
        LOGGER.fine(() -> "Extracted value is null, size is 1 byte for null marker");
        return Byte.BYTES; // size of the null marker only
      } else {
        LOGGER.fine(() -> "Extracted value is not null, delegating to sizer " + delegate.hashCode());
        return Byte.BYTES + delegate.applyAsInt(value); // size of not-null marker + value size
      }
    };
  }

  /// Create writer for custom value types
  static BiConsumer<ByteBuffer, Object> createCustomWriter(MethodHandle getter, SerdeHandler handler) {
    final var marker = handler.marker();
    final var writer = handler.writer();
    // Use extractAndDelegate for null handling
    return extractAndDelegate((buffer, value) -> {
      ZigZagEncoding.putInt(buffer, marker);
      writer.accept(buffer, value);
    }, getter);
  }

  /// Create sizer for custom value types
  static ToIntFunction<Object> createCustomSizer(MethodHandle getter, @SuppressWarnings("auxiliaryclass") SerdeHandler handler) {
    // Use extractAndDelegate for null handling
    return extractAndDelegate((value) -> 
        ZigZagEncoding.sizeOf(handler.marker()) + handler.sizer().applyAsInt(value), 
        getter);
  }

  /// Create writer for reference value types (boxed primitives, String, etc.)
  static BiConsumer<ByteBuffer, Object> createRefValueWriter(
      TypeExpr2.RefValueType refType,
      MethodHandle getter,
      Class<?> javaType,
      Function<Class<?>, BiConsumer<ByteBuffer, Object>> typeWriterResolver
  ) {
    // Use extractAndDelegate for null handling
    return extractAndDelegate((buffer, value) -> {
        // Write the appropriate marker based on refType
        switch (refType) {
          case BOOLEAN -> {
            ZigZagEncoding.putInt(buffer, TypeExpr2.referenceToMarker(Boolean.class));
            buffer.put((byte) ((Boolean) value ? 1 : 0));
          }
          case BYTE -> {
            ZigZagEncoding.putInt(buffer, TypeExpr2.referenceToMarker(Byte.class));
            buffer.put((Byte) value);
          }
          case SHORT -> {
            ZigZagEncoding.putInt(buffer, TypeExpr2.referenceToMarker(Short.class));
            buffer.putShort((Short) value);
          }
          case CHARACTER -> {
            ZigZagEncoding.putInt(buffer, TypeExpr2.referenceToMarker(Character.class));
            buffer.putChar((Character) value);
          }
          case INTEGER -> {
            int intValue = (Integer) value;
            if (intValue >= -1 && intValue <= 125) {
              ZigZagEncoding.putInt(buffer, TypeExpr2.referenceToMarker(Integer.class));
              buffer.put((byte) intValue);
            } else {
              ZigZagEncoding.putInt(buffer, -7);
              ZigZagEncoding.putInt(buffer, intValue);
            }
          }
          case LONG -> {
            long longValue = (Long) value;
            if (longValue >= -1 && longValue <= 125) {
              ZigZagEncoding.putInt(buffer, TypeExpr2.referenceToMarker(Long.class));
              buffer.put((byte) longValue);
            } else {
              ZigZagEncoding.putInt(buffer, -9);
              ZigZagEncoding.putLong(buffer, longValue);
            }
          }
          case FLOAT -> {
            ZigZagEncoding.putInt(buffer, TypeExpr2.referenceToMarker(Float.class));
            buffer.putFloat((Float) value);
          }
          case DOUBLE -> {
            ZigZagEncoding.putInt(buffer, TypeExpr2.referenceToMarker(Double.class));
            buffer.putDouble((Double) value);
          }
          case STRING -> {
            ZigZagEncoding.putInt(buffer, TypeExpr2.referenceToMarker(String.class));
            byte[] bytes = ((String) value).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            ZigZagEncoding.putInt(buffer, bytes.length);
            buffer.put(bytes);
          }
          case RECORD, ENUM, INTERFACE -> {
            // Delegate to type-specific writer
            BiConsumer<ByteBuffer, Object> writer = typeWriterResolver.apply(javaType);
            writer.accept(buffer, value);
          }
          case LOCAL_DATE, LOCAL_DATE_TIME, CUSTOM ->
              throw new UnsupportedOperationException("Type not yet supported: " + refType);
        }
    }, getter);
  }

  /// Create reader for reference value types
  static Function<ByteBuffer, Object> createRefValueReader(
      TypeExpr2.RefValueType refType,
      Class<?> javaType,
      Function<Class<?>, Function<ByteBuffer, Object>> typeReaderResolver
  ) {
    return buffer -> {
      // First read the null marker
      byte nullMarker = buffer.get();
      if (nullMarker == NULL_MARKER) {
        return null;
      } else if (nullMarker != NOT_NULL_MARKER) {
        throw new IllegalStateException("Invalid null marker: " + nullMarker);
      }
      
      // Now read the type marker
      int marker = ZigZagEncoding.getInt(buffer);

      return switch (refType) {
        case BOOLEAN -> {
          assert marker == TypeExpr2.referenceToMarker(Boolean.class) : "Expected BOOLEAN marker, got " + marker;
          yield buffer.get() != 0;
        }
        case BYTE -> {
          assert marker == TypeExpr2.referenceToMarker(Byte.class) : "Expected BYTE marker, got " + marker;
          yield buffer.get();
        }
        case SHORT -> {
          assert marker == TypeExpr2.referenceToMarker(Short.class) : "Expected SHORT marker, got " + marker;
          yield buffer.getShort();
        }
        case CHARACTER -> {
          assert marker == TypeExpr2.referenceToMarker(Character.class) : "Expected CHARACTER marker, got " + marker;
          yield buffer.getChar();
        }
        case INTEGER -> {
          if (marker == TypeExpr2.referenceToMarker(Integer.class)) {
            yield (int) buffer.get();
          } else if (marker == -7) {
            yield ZigZagEncoding.getInt(buffer);
          } else {
            throw new IllegalStateException("Expected INTEGER marker, got " + marker);
          }
        }
        case LONG -> {
          if (marker == TypeExpr2.referenceToMarker(Long.class)) {
            yield (long) buffer.get();
          } else if (marker == -9) {
            yield ZigZagEncoding.getLong(buffer);
          } else {
            throw new IllegalStateException("Expected LONG marker, got " + marker);
          }
        }
        case FLOAT -> {
          assert marker == TypeExpr2.referenceToMarker(Float.class) : "Expected FLOAT marker, got " + marker;
          yield buffer.getFloat();
        }
        case DOUBLE -> {
          assert marker == TypeExpr2.referenceToMarker(Double.class) : "Expected DOUBLE marker, got " + marker;
          yield buffer.getDouble();
        }
        case STRING -> {
          assert marker == TypeExpr2.referenceToMarker(String.class) : "Expected STRING marker, got " + marker;
          int length = ZigZagEncoding.getInt(buffer);
          byte[] bytes = new byte[length];
          buffer.get(bytes);
          yield new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }
        case RECORD, ENUM, INTERFACE -> {
          // Put back the marker for the type-specific reader
          buffer.position(buffer.position() - ZigZagEncoding.sizeOf(marker));
          Function<ByteBuffer, Object> reader = typeReaderResolver.apply(javaType);
          yield reader.apply(buffer);
        }
        case LOCAL_DATE, LOCAL_DATE_TIME, CUSTOM ->
            throw new UnsupportedOperationException("Type not yet supported: " + refType);
      };
    };
  }

  /// Create sizer for reference value types
  static ToIntFunction<Object> createRefValueSizer(
      TypeExpr2.RefValueType refType,
      MethodHandle getter,
      Class<?> javaType,
      Function<Class<?>, ToIntFunction<Object>> typeSizerResolver
  ) {
    // Use extractAndDelegate for null handling
    return extractAndDelegate((value) -> {
        return switch (refType) {
          case BOOLEAN -> ZigZagEncoding.sizeOf(TypeExpr2.referenceToMarker(Boolean.class)) + Byte.BYTES;
          case BYTE -> ZigZagEncoding.sizeOf(TypeExpr2.referenceToMarker(Byte.class)) + Byte.BYTES;
          case SHORT -> ZigZagEncoding.sizeOf(TypeExpr2.referenceToMarker(Short.class)) + Short.BYTES;
          case CHARACTER -> ZigZagEncoding.sizeOf(TypeExpr2.referenceToMarker(Character.class)) + Character.BYTES;
          case INTEGER -> {
            int intValue = (Integer) value;
            if (intValue >= -1 && intValue <= 125) {
              yield ZigZagEncoding.sizeOf(TypeExpr2.referenceToMarker(Integer.class)) + Byte.BYTES;
            } else {
              yield ZigZagEncoding.sizeOf(-7) + ZigZagEncoding.sizeOf(intValue);
            }
          }
          case LONG -> {
            long longValue = (Long) value;
            if (longValue >= -1 && longValue <= 125) {
              yield ZigZagEncoding.sizeOf(TypeExpr2.referenceToMarker(Long.class)) + Byte.BYTES;
            } else {
              yield ZigZagEncoding.sizeOf(-9) + ZigZagEncoding.sizeOf(longValue);
            }
          }
          case FLOAT -> ZigZagEncoding.sizeOf(TypeExpr2.referenceToMarker(Float.class)) + Float.BYTES;
          case DOUBLE -> ZigZagEncoding.sizeOf(TypeExpr2.referenceToMarker(Double.class)) + Double.BYTES;
          case STRING -> {
            String str = (String) value;
            byte[] bytes = str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            yield ZigZagEncoding.sizeOf(TypeExpr2.referenceToMarker(String.class)) +
                ZigZagEncoding.sizeOf(bytes.length) + bytes.length;
          }
          case RECORD, ENUM, INTERFACE -> {
            ToIntFunction<Object> sizer = typeSizerResolver.apply(javaType);
            yield sizer.applyAsInt(value);
          }
          case LOCAL_DATE, LOCAL_DATE_TIME, CUSTOM ->
              throw new UnsupportedOperationException("Type not yet supported: " + refType);
        };
    }, getter);
  }

  /// Empty enum to seal the interface
  enum Nothing implements Companion2 {}
}
