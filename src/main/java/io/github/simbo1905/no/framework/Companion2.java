// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;

interface Sizer extends ToIntFunction<Object> {
  /// Get the size of an object of the given class
  default int sizeOf(Object obj) {
    return applyAsInt(obj);
  }
}

interface SizerResolver extends
    Function<Class<?>, Sizer> {
}

interface Writer extends
    BiConsumer<ByteBuffer, Object> {

  /// Write an object to the ByteBuffer
  default void write(ByteBuffer buffer, Object obj) {
    accept(buffer, obj);
  }
}

interface WriterResolver extends
    Function<Class<?>, Writer> {
  default Writer resolveWriter(Class<?> type) {
    return apply(type);
  }
}

interface Reader extends
    Function<ByteBuffer, Object> {

  /// Read an object from the ByteBuffer
  default Object read(ByteBuffer buffer) {
    return apply(buffer);
  }
}

interface ReaderResolver extends
    Function<Long, Reader> {
  default Reader resolveReader(long typeSignature, ByteBuffer buffer) {
    return apply(typeSignature);
  }
}

/// Companion for TypeExpr2-based component serde building
@SuppressWarnings("auxiliaryclass")
sealed interface Companion2 permits Companion2.Nothing {

  /// Build ComponentSerde array for a record type using TypeExpr2 AST
  @SuppressWarnings("auxiliaryclass")
  static ComponentSerde[] buildComponentSerdes(
      Class<?> recordClass,
      Collection<SerdeHandler> customHandlers,
      SizerResolver typeSizerResolver,
      WriterResolver typeWriterResolver,
      ReaderResolver typeReaderResolver
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

          Writer writer = createComponentWriter(typeExpr, getter, customHandlers, typeWriterResolver);
          Reader reader = createComponentReader(typeExpr, customHandlers, typeReaderResolver);
          Sizer sizer = createComponentSizer(typeExpr, getter, customHandlers, typeSizerResolver);

          return new ComponentSerde(writer, reader, sizer);
        })
        .toArray(ComponentSerde[]::new);
  }

  /// Create writer for a component based on its TypeExpr2
  static Writer createComponentWriter(
      TypeExpr2 typeExpr,
      MethodHandle getter,
      @SuppressWarnings("auxiliaryclass") Collection<SerdeHandler> customHandlers,
      WriterResolver typeWriterResolver
  ) {
    return switch (typeExpr) {
      case TypeExpr2.PrimitiveValueNode(var type, var javaType) -> createPrimitiveWriter(type, getter);

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
      case TypeExpr2.ListNode(var elementNode) ->
          createListWriter(elementNode, getter, customHandlers, typeWriterResolver);
      case TypeExpr2.OptionalNode(var wrappedType) ->
          createOptionalWriter(wrappedType, getter, customHandlers, typeWriterResolver);
      case TypeExpr2.MapNode(var keyNode, var valueNode) ->
          createMapWriter(keyNode, valueNode, getter, customHandlers, typeWriterResolver);
    };
  }

  /// Create value reader without null checking (for Optional and other containers)
  static Reader createValueReaderWithoutNullCheck(
      TypeExpr2 typeExpr,
      Collection<SerdeHandler> customHandlers,
      ReaderResolver typeReaderResolver
  ) {
    return switch (typeExpr) {
      case TypeExpr2.PrimitiveValueNode(final var type, final var javaType) -> createPrimitiveReader(type);

      case TypeExpr2.RefValueNode(final var refType, final var javaType, final var marker) -> {
        if (refType == TypeExpr2.RefValueType.CUSTOM) {
          final var handler = customHandlers.stream()
              .filter(h -> h.valueBasedLike().equals(javaType))
              .findFirst()
              .orElseThrow(() -> new IllegalStateException("No handler for custom type: " + javaType));
          yield buffer -> {
            final var typeMarker = ZigZagEncoding.getInt(buffer);
            assert typeMarker == handler.marker() : "Expected marker " + handler.marker() + " but got " + typeMarker;
            return handler.reader().apply(buffer);
          };
        } else {
          yield createRefValueReader(refType, (Class<?>) javaType, typeReaderResolver);
        }
      }

      case TypeExpr2.ArrayNode ignored ->
          throw new UnsupportedOperationException("Array value readers not yet implemented");
      case TypeExpr2.ListNode(var elementNode) -> createListReader(elementNode, customHandlers, typeReaderResolver);
      case TypeExpr2.OptionalNode(final var wrappedType) ->
          createOptionalReader(wrappedType, customHandlers, typeReaderResolver);
      case TypeExpr2.MapNode(var keyNode, var valueNode) ->
          createMapReader(keyNode, valueNode, customHandlers, typeReaderResolver);
    };
  }

  /// Create reader for a component based on its TypeExpr2
  static Reader createComponentReader(
      TypeExpr2 typeExpr,
      @SuppressWarnings("auxiliaryclass") Collection<SerdeHandler> customHandlers,
      ReaderResolver typeReaderResolver
  ) {
    return switch (typeExpr) {
      case TypeExpr2.PrimitiveValueNode ignored -> // Primitives cannot be null, so no null check needed
          createValueReaderWithoutNullCheck(typeExpr, customHandlers, typeReaderResolver);

      case TypeExpr2.RefValueNode(final var refType, final var javaType, final var marker) -> {
        // Get the value reader without null checking
        final var valueReader = createValueReaderWithoutNullCheck(typeExpr, customHandlers, typeReaderResolver);

        // Wrap the value reader with a null check for reference types
        yield buffer -> {
          final var positionBefore = buffer.position();
          final var nullMarker = buffer.get();
          LOGGER.fine(() -> "createComponentReader: At position " + positionBefore + " read nullMarker=" + nullMarker + " (NULL=" + NULL_MARKER + ", NOT_NULL=" + NOT_NULL_MARKER + ")");
          if (nullMarker == NULL_MARKER) {
            LOGGER.fine(() -> "createComponentReader: Found NULL_MARKER, returning null");
            return null;
          } else if (nullMarker == NOT_NULL_MARKER) {
            LOGGER.fine(() -> "createComponentReader: Found NOT_NULL_MARKER, delegating to valueReader");
            return valueReader.apply(buffer);
          } else {
            throw new IllegalStateException("Invalid null marker: " + nullMarker);
          }
        };
      }

      case TypeExpr2.ArrayNode ignored ->
          throw new UnsupportedOperationException("Array component readers not yet implemented");
      case TypeExpr2.ListNode(var elementNode) -> createListReader(elementNode, customHandlers, typeReaderResolver);
      case TypeExpr2.OptionalNode(final var wrappedType) ->
          createOptionalReader(wrappedType, customHandlers, typeReaderResolver);
      case TypeExpr2.MapNode(var keyNode, var valueNode) ->
          createMapReader(keyNode, valueNode, customHandlers, typeReaderResolver);
    };
  }

  /// Create sizer for a component based on its TypeExpr2
  static Sizer createComponentSizer(
      TypeExpr2 typeExpr,
      MethodHandle getter,
      @SuppressWarnings("auxiliaryclass") Collection<SerdeHandler> customHandlers,
      SizerResolver typeSizerResolver
  ) {
    return switch (typeExpr) {
      case TypeExpr2.PrimitiveValueNode(var type, var javaType) -> createPrimitiveSizer(type);

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
      case TypeExpr2.ListNode(var elementNode) ->
          createListSizer(elementNode, getter, customHandlers, typeSizerResolver);
      case TypeExpr2.OptionalNode(var wrappedType) ->
          createOptionalSizer(wrappedType, getter, customHandlers, typeSizerResolver);
      case TypeExpr2.MapNode(var keyNode, var valueNode) ->
          createMapSizer(keyNode, valueNode, getter, customHandlers, typeSizerResolver);
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
  static Writer createPrimitiveWriter(TypeExpr2.PrimitiveValueType type, MethodHandle getter) {
    return (buffer, obj) -> {
      try {
        switch (type) {
          case TypeExpr2.PrimitiveValueType.SimplePrimitive(var name, var marker) -> {
            switch (name) {
              case "BOOLEAN" -> buffer.put((byte) ((boolean) getter.invoke(obj) ? 1 : 0));
              case "BYTE" -> buffer.put((byte) getter.invoke(obj));
              case "SHORT" -> buffer.putShort((short) getter.invoke(obj));
              case "CHARACTER" -> buffer.putChar((char) getter.invoke(obj));
              case "FLOAT" -> buffer.putFloat((float) getter.invoke(obj));
              case "DOUBLE" -> buffer.putDouble((double) getter.invoke(obj));
              default -> throw new IllegalStateException("Unknown primitive type: " + name);
            }
          }
          case TypeExpr2.PrimitiveValueType.IntegerType(var fixedMarker, var varMarker) -> {
            int result = (int) getter.invoke(obj);
            int position = buffer.position();
            int zigzagSize = ZigZagEncoding.sizeOf(result);
            LOGGER.fine(() -> "Writing primitive int value=" + result + " at position=" + position + " zigzagSize=" + zigzagSize + " vs Integer.BYTES=" + Integer.BYTES);
            if (zigzagSize < Integer.BYTES) {
              LOGGER.fine(() -> "Using INTEGER_VAR marker=" + varMarker);
              ZigZagEncoding.putInt(buffer, varMarker);
              ZigZagEncoding.putInt(buffer, result);
            } else {
              LOGGER.fine(() -> "Using INTEGER marker=" + fixedMarker);
              ZigZagEncoding.putInt(buffer, fixedMarker);
              buffer.putInt(result);
            }
            LOGGER.fine(() -> "Wrote " + (buffer.position() - position) + " bytes total");
          }
          case TypeExpr2.PrimitiveValueType.LongType(var fixedMarker, var varMarker) -> {
            long result = (long) getter.invoke(obj);
            int position = buffer.position();
            int zigzagSize = ZigZagEncoding.sizeOf(result);
            LOGGER.fine(() -> "Writing primitive long value=" + result + " at position=" + position + " zigzagSize=" + zigzagSize + " vs Long.BYTES=" + Long.BYTES);
            if (zigzagSize < Long.BYTES) {
              LOGGER.fine(() -> "Using LONG_VAR marker=" + varMarker);
              ZigZagEncoding.putInt(buffer, varMarker);
              ZigZagEncoding.putLong(buffer, result);
            } else {
              LOGGER.fine(() -> "Using LONG marker=" + fixedMarker);
              ZigZagEncoding.putInt(buffer, fixedMarker);
              buffer.putLong(result);
            }
            LOGGER.fine(() -> "Wrote " + (buffer.position() - position) + " bytes total");
          }
        }
      } catch (Throwable e) {
        throw new IllegalStateException("Failed to write primitive", e);
      }
    };
  }

  /// Create reader for primitive types
  static Reader createPrimitiveReader(TypeExpr2.PrimitiveValueType type) {
    return buffer -> switch (type) {
      case TypeExpr2.PrimitiveValueType.SimplePrimitive(var name, var marker) -> {
        yield switch (name) {
          case "BOOLEAN" -> buffer.get() != 0;
          case "BYTE" -> buffer.get();
          case "SHORT" -> buffer.getShort();
          case "CHARACTER" -> buffer.getChar();
          case "FLOAT" -> buffer.getFloat();
          case "DOUBLE" -> buffer.getDouble();
          default -> throw new IllegalStateException("Unknown primitive type: " + name);
        };
      }
      case TypeExpr2.PrimitiveValueType.IntegerType(var fixedMarker, var varMarker) -> {
        // Read the marker to determine encoding
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker == fixedMarker) {
          yield buffer.getInt();
        } else if (marker == varMarker) {
          yield ZigZagEncoding.getInt(buffer);
        } else {
          throw new IllegalStateException("Expected INTEGER marker " + fixedMarker + " or " + varMarker + ", got " + marker);
        }
      }
      case TypeExpr2.PrimitiveValueType.LongType(var fixedMarker, var varMarker) -> {
        // Read the marker to determine encoding
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker == fixedMarker) {
          yield buffer.getLong();
        } else if (marker == varMarker) {
          yield ZigZagEncoding.getLong(buffer);
        } else {
          throw new IllegalStateException("Expected LONG marker " + fixedMarker + " or " + varMarker + ", got " + marker);
        }
      }
    };
  }

  Sizer byteSizer = (o) -> Byte.BYTES;

  /// Create sizer for primitive types
  static Sizer createPrimitiveSizer(TypeExpr2.PrimitiveValueType type) {
    // Primitive types don't need the record object, just return worst-case sizes
    return switch (type) {
      case TypeExpr2.PrimitiveValueType.SimplePrimitive(var name, var marker) -> switch (name) {
        case "BOOLEAN", "BYTE" -> (o) -> Byte.BYTES;
        case "SHORT", "CHARACTER" -> (o) -> Short.BYTES;
        case "FLOAT" -> (o) -> Float.BYTES;
        case "DOUBLE" -> (o) -> Double.BYTES;
        default -> throw new IllegalStateException("Unknown primitive type: " + name);
      };
      case TypeExpr2.PrimitiveValueType.IntegerType ignored -> (o) ->
          // Worst case: 1 byte marker + 4 bytes data
          ZigZagEncoding.sizeOf(-6) + Integer.BYTES;
      case TypeExpr2.PrimitiveValueType.LongType ignored -> (o) ->
          // Worst case: 1 byte marker + 8 bytes data
          ZigZagEncoding.sizeOf(-8) + Long.BYTES;
    };
  }

  /// Constants for null markers
  byte NULL_MARKER = Byte.MIN_VALUE;
  byte NOT_NULL_MARKER = Byte.MAX_VALUE;

  /// Extract value from record using getter and delegate to writer with null handling
  static Writer extractAndDelegate(Writer delegate, MethodHandle accessor) {
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
  static Sizer extractAndDelegate(Sizer delegate, MethodHandle accessor) {
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
  static Writer createCustomWriter(MethodHandle getter, SerdeHandler handler) {
    final var marker = handler.marker();
    final var writer = handler.writer();
    // Use extractAndDelegate for null handling
    return extractAndDelegate((buffer, value) -> {
      ZigZagEncoding.putInt(buffer, marker);
      writer.accept(buffer, value);
    }, getter);
  }

  /// Create sizer for custom value types
  static Sizer createCustomSizer(MethodHandle getter, @SuppressWarnings("auxiliaryclass") SerdeHandler handler) {
    // Use extractAndDelegate for null handling
    return extractAndDelegate((value) ->
            ZigZagEncoding.sizeOf(handler.marker()) + handler.sizer().applyAsInt(value),
        getter);
  }

  /// Create writer for reference value types (boxed primitives, String, etc.)
  static Writer createRefValueWriter(
      TypeExpr2.RefValueType refType,
      MethodHandle getter,
      Class<?> javaType,
      WriterResolver typeWriterResolver
  ) {
    // Use extractAndDelegate for null handling. The returned delegate handles the non-null value.
    return extractAndDelegate((buffer, value) -> {
      switch (refType) {
        case RECORD, ENUM, INTERFACE -> {
          // For user types, we don't write a marker. We delegate to the resolver,
          // which is responsible for writing the 'long' signature.
          typeWriterResolver.resolveWriter(javaType).accept(buffer, value);
        }
        default -> {
          // For built-in ref types, write the 'int' marker first, then the data.
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
              if (ZigZagEncoding.sizeOf(intValue) < Integer.BYTES) { // Tiny int optimization
                ZigZagEncoding.putInt(buffer, ((TypeExpr2.PrimitiveValueType.IntegerType) TypeExpr2.PrimitiveValueType.INTEGER).varMarker()); // Varint marker
                ZigZagEncoding.putInt(buffer, intValue);
              } else {
                ZigZagEncoding.putInt(buffer, TypeExpr2.referenceToMarker(Integer.class));
                buffer.putInt(intValue);
              }
            }
            case LONG -> {
              long longValue = (Long) value;
              if (ZigZagEncoding.sizeOf(longValue) < Long.BYTES) { // Tiny long optimization
                ZigZagEncoding.putInt(buffer, ((TypeExpr2.PrimitiveValueType.LongType) TypeExpr2.PrimitiveValueType.LONG).varMarker()); // Varlong marker
                ZigZagEncoding.putLong(buffer, longValue);
              } else {
                ZigZagEncoding.putInt(buffer, TypeExpr2.referenceToMarker(Long.class));
                buffer.putLong(longValue);
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
            case LOCAL_DATE, LOCAL_DATE_TIME, CUSTOM ->
                throw new UnsupportedOperationException("Type not yet supported: " + refType);
          }
        }
      }
    }, getter);
  }

  /// Create reader for reference value types
  static Reader createRefValueReader(
      TypeExpr2.RefValueType refType,
      Class<?> javaType,
      ReaderResolver typeReaderResolver
  ) {
    // Meta-programming time decision - return focused functions with no runtime switches
    return switch (refType) {
      case RECORD, ENUM, INTERFACE -> buffer -> {
        final var typeSignature = buffer.getLong();
        // The resolver returns a Reader for the specific type, which we must then apply to the buffer.
        return typeReaderResolver.apply(typeSignature).apply(buffer);
      };

      case BOOLEAN -> {
        final var expectedMarker = TypeExpr2.referenceToMarker(Boolean.class);
        yield buffer -> {
          final var marker = ZigZagEncoding.getInt(buffer);
          assert marker == expectedMarker : "Expected BOOLEAN marker " + expectedMarker + ", got " + marker;
          return buffer.get() != 0;
        };
      }

      case BYTE -> {
        final var expectedMarker = TypeExpr2.referenceToMarker(Byte.class);
        yield buffer -> {
          final var marker = ZigZagEncoding.getInt(buffer);
          assert marker == expectedMarker : "Expected BYTE marker " + expectedMarker + ", got " + marker;
          return buffer.get();
        };
      }

      case SHORT -> {
        final var expectedMarker = TypeExpr2.referenceToMarker(Short.class);
        yield buffer -> {
          final var marker = ZigZagEncoding.getInt(buffer);
          assert marker == expectedMarker : "Expected SHORT marker " + expectedMarker + ", got " + marker;
          return buffer.getShort();
        };
      }

      case CHARACTER -> {
        final var expectedMarker = TypeExpr2.referenceToMarker(Character.class);
        yield buffer -> {
          final var marker = ZigZagEncoding.getInt(buffer);
          assert marker == expectedMarker : "Expected CHARACTER marker " + expectedMarker + ", got " + marker;
          return buffer.getChar();
        };
      }

      case INTEGER -> {
        final var fixedMarker = TypeExpr2.referenceToMarker(Integer.class);
        final var varMarker = ((TypeExpr2.PrimitiveValueType.IntegerType) TypeExpr2.PrimitiveValueType.INTEGER).varMarker();
        yield buffer -> {
          final var marker = ZigZagEncoding.getInt(buffer);
          if (marker == fixedMarker) {
            return buffer.getInt();
          } else if (marker == varMarker) {
            return ZigZagEncoding.getInt(buffer);
          } else {
            throw new IllegalStateException("Expected INTEGER marker " + fixedMarker + " or " + varMarker + ", got " + marker);
          }
        };
      }

      case LONG -> {
        final var fixedMarker = TypeExpr2.referenceToMarker(Long.class);
        final var varMarker = ((TypeExpr2.PrimitiveValueType.LongType) TypeExpr2.PrimitiveValueType.LONG).varMarker();
        yield buffer -> {
          final var marker = ZigZagEncoding.getInt(buffer);
          if (marker == fixedMarker) {
            return buffer.getLong();
          } else if (marker == varMarker) {
            return ZigZagEncoding.getLong(buffer);
          } else {
            throw new IllegalStateException("Expected LONG marker " + fixedMarker + " or " + varMarker + ", got " + marker);
          }
        };
      }

      case FLOAT -> {
        final var expectedMarker = TypeExpr2.referenceToMarker(Float.class);
        yield buffer -> {
          final var marker = ZigZagEncoding.getInt(buffer);
          assert marker == expectedMarker : "Expected FLOAT marker " + expectedMarker + ", got " + marker;
          return buffer.getFloat();
        };
      }

      case DOUBLE -> {
        final var expectedMarker = TypeExpr2.referenceToMarker(Double.class);
        yield buffer -> {
          final var marker = ZigZagEncoding.getInt(buffer);
          assert marker == expectedMarker : "Expected DOUBLE marker " + expectedMarker + ", got " + marker;
          return buffer.getDouble();
        };
      }

      case STRING -> {
        final var expectedMarker = TypeExpr2.referenceToMarker(String.class);
        yield buffer -> {
          final var marker = ZigZagEncoding.getInt(buffer);
          assert marker == expectedMarker : "Expected STRING marker " + expectedMarker + ", got " + marker;
          final var length = ZigZagEncoding.getInt(buffer);
          final var bytes = new byte[length];
          buffer.get(bytes);
          return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        };
      }

      case LOCAL_DATE, LOCAL_DATE_TIME, CUSTOM ->
          throw new UnsupportedOperationException("Type not yet supported: " + refType);
    };
  }

  /// Create sizer for reference value types
  static Sizer createRefValueSizer(
      TypeExpr2.RefValueType refType,
      MethodHandle getter,
      Class<?> javaType,
      SizerResolver typeSizerResolver
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
            yield ZigZagEncoding.sizeOf(((TypeExpr2.PrimitiveValueType.IntegerType) TypeExpr2.PrimitiveValueType.INTEGER).varMarker()) + ZigZagEncoding.sizeOf(intValue);
          }
        }
        case LONG -> {
          long longValue = (Long) value;
          if (longValue >= -1 && longValue <= 125) {
            yield ZigZagEncoding.sizeOf(TypeExpr2.referenceToMarker(Long.class)) + Byte.BYTES;
          } else {
            yield ZigZagEncoding.sizeOf(((TypeExpr2.PrimitiveValueType.LongType) TypeExpr2.PrimitiveValueType.LONG).varMarker()) + ZigZagEncoding.sizeOf(longValue);
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
          // Delegate to the main pickler's sizer resolver
          Sizer sizer = typeSizerResolver.apply(javaType);
          yield sizer.applyAsInt(value); // The extractAndDelegate wrapper will handle nulls
        }
        case LOCAL_DATE, LOCAL_DATE_TIME, CUSTOM ->
            throw new UnsupportedOperationException("Type not yet supported: " + refType);
      };
    }, getter);
  }

  String SHA_256 = "SHA-256";

  /// Compute type signatures for all record classes using streams using the generic type information
  static Map<Class<?>, Long> computeRecordTypeSignatures(List<Class<?>> recordClasses) {
    return recordClasses.stream()
        .collect(Collectors.toMap(
            clz -> clz,
            clz -> {
              final var components = clz.getRecordComponents();
              // NOTE: This will need to be adapted to use TypeExpr2
              final var typeExprs = Arrays.stream(components)
                  .map(comp -> TypeExpr2.analyzeType(comp.getGenericType(), List.of())) // Adapt to TypeExpr2
                  .toArray(TypeExpr2[]::new);
              return hashClassSignature(clz, components, typeExprs);
            }
        ));
  }

  /// Compute a type signature from full class name, the component types, and component name
  static long hashClassSignature(Class<?> clazz, RecordComponent[] components, TypeExpr2[] componentTypes) { // Adapt to TypeExpr2
    String input = Stream.concat(Stream.of(clazz.getSimpleName()),
            IntStream.range(0, components.length).boxed().flatMap(i ->
                Stream.concat(Stream.of(componentTypes[i].toTreeString()), Stream.of(components[i].getName()))))
        .collect(Collectors.joining("!"));
    return hashSignature(input);
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
    result = IntStream.range(0, Long.BYTES).mapToLong(i -> (hash[i] & 0xFFL) << (56 - i * 8)).reduce(0L, (a, b) -> a | b);
    return result;
  }

  /// Create writer for Optional types
  static Writer createOptionalWriter(
      TypeExpr2 wrappedType,
      MethodHandle getter,
      Collection<SerdeHandler> customHandlers,
      WriterResolver typeWriterResolver
  ) {
    return (buffer, record) -> {
      try {
        java.util.Optional<?> optional = (java.util.Optional<?>) getter.invoke(record);
        if (optional.isEmpty()) {
          LOGGER.fine(() -> "Writing OPTIONAL_EMPTY marker");
          ZigZagEncoding.putInt(buffer, TypeExpr2.ContainerType.OPTIONAL_EMPTY.marker());
        } else {
          LOGGER.fine(() -> "Writing OPTIONAL_OF marker then value");
          ZigZagEncoding.putInt(buffer, TypeExpr2.ContainerType.OPTIONAL_OF.marker());
          // Write the wrapped value directly
          Object value = optional.get();
          writeValue(buffer, value, wrappedType, customHandlers, typeWriterResolver);
        }
      } catch (Throwable e) {
        throw new IllegalStateException("Failed to write Optional", e);
      }
    };
  }

  /// Create reader for Optional types
  static Reader createOptionalReader(
      TypeExpr2 wrappedType,
      Collection<SerdeHandler> customHandlers,
      ReaderResolver typeReaderResolver
  ) {
    // Create reader for the wrapped value WITHOUT null checking (Optional handles null at container level)
    Reader wrappedReader = createValueReaderWithoutNullCheck(wrappedType, customHandlers, typeReaderResolver);

    return buffer -> {
      final var positionBefore = buffer.position();
      final var marker = ZigZagEncoding.getInt(buffer);
      LOGGER.fine(() -> "createOptionalReader: At position " + positionBefore + " read marker=" + marker + " (EMPTY=" + TypeExpr2.ContainerType.OPTIONAL_EMPTY.marker() + ", OF=" + TypeExpr2.ContainerType.OPTIONAL_OF.marker() + ")");
      if (marker == TypeExpr2.ContainerType.OPTIONAL_EMPTY.marker()) {
        LOGGER.fine(() -> "Reading OPTIONAL_EMPTY");
        return java.util.Optional.empty();
      } else if (marker == TypeExpr2.ContainerType.OPTIONAL_OF.marker()) {
        LOGGER.fine(() -> "Reading OPTIONAL_OF then value");
        Object value = wrappedReader.apply(buffer);
        LOGGER.fine(() -> "createOptionalReader: Read wrapped value=" + value);
        return java.util.Optional.of(value);
      } else {
        throw new IllegalStateException("Expected OPTIONAL_EMPTY or OPTIONAL_OF marker, got: " + marker);
      }
    };
  }

  /// Create sizer for Optional types
  static Sizer createOptionalSizer(
      TypeExpr2 wrappedType,
      MethodHandle getter,
      Collection<SerdeHandler> customHandlers,
      SizerResolver typeSizerResolver
  ) {
    return record -> {
      try {
        java.util.Optional<?> optional = (java.util.Optional<?>) getter.invoke(record);
        if (optional.isEmpty()) {
          // Size of OPTIONAL_EMPTY marker only
          return ZigZagEncoding.sizeOf(TypeExpr2.ContainerType.OPTIONAL_EMPTY.marker());
        } else {
          // Size of OPTIONAL_OF marker + wrapped value size
          Object value = optional.get();
          int wrappedSize = sizeValue(value, wrappedType, customHandlers, typeSizerResolver);
          return ZigZagEncoding.sizeOf(TypeExpr2.ContainerType.OPTIONAL_OF.marker()) + wrappedSize;
        }
      } catch (Throwable e) {
        throw new IllegalStateException("Failed to size Optional", e);
      }
    };
  }

  /// Create writer for List types
  static Writer createListWriter(
      TypeExpr2 elementType,
      MethodHandle getter,
      Collection<SerdeHandler> customHandlers,
      WriterResolver typeWriterResolver
  ) {
    return (buffer, record) -> {
      try {
        @SuppressWarnings("unchecked")
        java.util.List<Object> list = (java.util.List<Object>) getter.invoke(record);
        if (list == null) {
          ZigZagEncoding.putInt(buffer, -1); // Use -1 to indicate a null list
        } else {
          ZigZagEncoding.putInt(buffer, list.size());
          for (Object item : list) {
            // Write list item with null handling - item is the value, not a record to extract from
            if (item == null) {
              buffer.put(NULL_MARKER);
            } else {
              buffer.put(NOT_NULL_MARKER);
              writeValue(buffer, item, elementType, customHandlers, typeWriterResolver);
            }
          }
        }
      } catch (Throwable e) {
        throw new IllegalStateException("Failed to write List", e);
      }
    };
  }

  /// Create reader for List types
  static Reader createListReader(
      TypeExpr2 elementType,
      Collection<SerdeHandler> customHandlers,
      ReaderResolver typeReaderResolver
  ) {
    final var elementReader = createComponentReader(elementType, customHandlers, typeReaderResolver);
    return buffer -> {
      final int size = ZigZagEncoding.getInt(buffer);
      if (size == -1) {
        return null;
      }
      final var list = new java.util.ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        list.add(elementReader.apply(buffer));
      }
      return list;
    };
  }

  /// Create writer for Map types
  static Writer createMapWriter(
      TypeExpr2 keyType,
      TypeExpr2 valueType,
      MethodHandle getter,
      Collection<SerdeHandler> customHandlers,
      WriterResolver typeWriterResolver
  ) {
    return (buffer, record) -> {
      try {
        @SuppressWarnings("unchecked")
        java.util.Map<Object, Object> map = (java.util.Map<Object, Object>) getter.invoke(record);

        if (map == null) {
          LOGGER.fine(() -> "Writing null map marker");
          ZigZagEncoding.putInt(buffer, -1); // Null map marker
          return;
        }

        LOGGER.fine(() -> "Writing map size: " + map.size());
        ZigZagEncoding.putInt(buffer, map.size());
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
          Object key = entry.getKey();
          Object value = entry.getValue();
          LOGGER.fine(() -> "Writing map entry: K=" + key + ", V=" + value);

          // Write key with null handling
          if (key == null) {
            LOGGER.finer(() -> "Writing NULL_MARKER for key");
            buffer.put(NULL_MARKER);
          } else {
            LOGGER.finer(() -> "Writing NOT_NULL_MARKER for key");
            buffer.put(NOT_NULL_MARKER);
            writeValue(buffer, key, keyType, customHandlers, typeWriterResolver);
          }

          // Write value with null handling
          if (value == null) {
            LOGGER.finer(() -> "Writing NULL_MARKER for value");
            buffer.put(NULL_MARKER);
          } else {
            LOGGER.finer(() -> "Writing NOT_NULL_MARKER for value");
            buffer.put(NOT_NULL_MARKER);
            writeValue(buffer, value, valueType, customHandlers, typeWriterResolver);
          }
        }
      } catch (Throwable e) {
        throw new IllegalStateException("Failed to write Map", e);
      }
    };
  }

  /// Create reader for Map types
  static Reader createMapReader(
      TypeExpr2 keyType,
      TypeExpr2 valueType,
      Collection<SerdeHandler> customHandlers,
      ReaderResolver typeReaderResolver
  ) {
    final Function<ByteBuffer, Object> keyReader = createComponentReader(keyType, customHandlers, typeReaderResolver);
    final Function<ByteBuffer, Object> valueReader = createComponentReader(valueType, customHandlers, typeReaderResolver);

    return buffer -> {
      final int size = ZigZagEncoding.getInt(buffer);
      LOGGER.fine(() -> "Reading map size: " + size);
      if (size == -1) {
        LOGGER.fine(() -> "Read null map marker, returning null");
        return null;
      }

      final var map = new java.util.HashMap<>(size);
      for (int i = 0; i < size; i++) {
        final int entryIndex = i;
        LOGGER.fine(() -> "Reading map entry " + entryIndex);
        Object key = keyReader.apply(buffer);
        LOGGER.fine(() -> "Read map key: " + key);
        Object value = valueReader.apply(buffer);
        LOGGER.fine(() -> "Read map value: " + value);
        map.put(key, value);
      }
      return map;
    };
  }

  /// Create sizer for Map types
  static Sizer createMapSizer(
      TypeExpr2 keyType,
      TypeExpr2 valueType,
      MethodHandle getter,
      Collection<SerdeHandler> customHandlers,
      SizerResolver typeSizerResolver
  ) {
    return record -> {
      try {
        @SuppressWarnings("unchecked")
        java.util.Map<Object, Object> map = (java.util.Map<Object, Object>) getter.invoke(record);
        if (map == null) {
          return ZigZagEncoding.sizeOf(-1);
        }

        int totalSize = ZigZagEncoding.sizeOf(map.size());
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
          // Key size
          totalSize += Byte.BYTES; // null marker
          if (entry.getKey() != null) {
            totalSize += sizeValue(entry.getKey(), keyType, customHandlers, typeSizerResolver);
          }

          // Value size
          totalSize += Byte.BYTES; // null marker
          if (entry.getValue() != null) {
            totalSize += sizeValue(entry.getValue(), valueType, customHandlers, typeSizerResolver);
          }
        }
        return totalSize;
      } catch (Throwable e) {
        throw new IllegalStateException("Failed to size Map", e);
      }
    };
  }

  /// Create sizer for List types
  static Sizer createListSizer(
      TypeExpr2 elementType,
      MethodHandle getter,
      Collection<SerdeHandler> customHandlers,
      SizerResolver typeSizerResolver
  ) {
    return record -> {
      try {
        @SuppressWarnings("unchecked")
        java.util.List<Object> list = (java.util.List<Object>) getter.invoke(record);
        if (list == null) {
          return ZigZagEncoding.sizeOf(-1);
        }
        int totalSize = ZigZagEncoding.sizeOf(list.size());
        for (Object item : list) {
          totalSize += Byte.BYTES; // For the null marker
          if (item != null) {
            totalSize += sizeValue(item, elementType, customHandlers, typeSizerResolver);
          }
        }
        return totalSize;
      } catch (Throwable e) {
        throw new IllegalStateException("Failed to size List", e);
      }
    };
  }

  /// Write a value directly based on its TypeExpr2
  static void writeValue(
      ByteBuffer buffer,
      Object value,
      TypeExpr2 typeExpr,
      Collection<SerdeHandler> customHandlers,
      WriterResolver typeWriterResolver
  ) {
    final var positionBefore = buffer.position();
    LOGGER.fine(() -> "writeValue: Starting at position " + positionBefore + " for value=" + value + " typeExpr=" + typeExpr.toTreeString());
    switch (typeExpr) {
      case TypeExpr2.PrimitiveValueNode(var type, var javaType) -> {
        // For primitive types, we can't have them in Optional, this should be a RefValueNode
        throw new IllegalStateException("Primitives cannot be in Optional, should be boxed: " + javaType);
      }
      case TypeExpr2.RefValueNode(var refType, var javaType, var marker) -> {
        if (refType == TypeExpr2.RefValueType.CUSTOM) {
          SerdeHandler handler = customHandlers.stream()
              .filter(h -> h.valueBasedLike().equals(javaType))
              .findFirst()
              .orElseThrow(() -> new IllegalStateException("No handler for custom type: " + javaType));
          ZigZagEncoding.putInt(buffer, handler.marker());
          handler.writer().accept(buffer, value);
        } else if (refType == TypeExpr2.RefValueType.RECORD || refType == TypeExpr2.RefValueType.ENUM || refType == TypeExpr2.RefValueType.INTERFACE) {
          // User types delegate to typeWriterResolver
          typeWriterResolver.resolveWriter((Class<?>) javaType).accept(buffer, value);
        } else {
          // Built-in reference types - create focused writer during construction time
          final var writer = createBuiltInRefWriter(refType);
          LOGGER.fine(() -> "writeValue: Writing built-in ref type " + refType + " value=" + value);
          writer.accept(buffer, value);
        }
      }
      case TypeExpr2.OptionalNode(var wrappedType) -> {
        // Nested Optional
        if (value instanceof java.util.Optional<?> nested) {
          if (nested.isEmpty()) {
            ZigZagEncoding.putInt(buffer, TypeExpr2.ContainerType.OPTIONAL_EMPTY.marker());
          } else {
            LOGGER.fine(() -> "writeValue: Writing OPTIONAL_OF marker then nested value");
            ZigZagEncoding.putInt(buffer, TypeExpr2.ContainerType.OPTIONAL_OF.marker());
            writeValue(buffer, nested.get(), wrappedType, customHandlers, typeWriterResolver);
          }
        } else {
          throw new IllegalStateException("Expected Optional but got: " + value.getClass());
        }
      }
      case TypeExpr2.ArrayNode arrayNode ->
          throw new UnsupportedOperationException("Array not yet implemented in writeValue");
      case TypeExpr2.ListNode listNode ->
          throw new UnsupportedOperationException("List not yet implemented in writeValue");
      case TypeExpr2.MapNode mapNode ->
          throw new UnsupportedOperationException("Map not yet implemented in writeValue");
    }
    final var positionAfter = buffer.position();
    final var bytesWritten = positionAfter - positionBefore;
    LOGGER.fine(() -> "writeValue: Finished, wrote " + bytesWritten + " bytes from position " + positionBefore + " to " + positionAfter);
  }

  /// Create focused writer function for built-in reference types (avoids switch on hot path)
  static BiConsumer<ByteBuffer, Object> createBuiltInRefWriter(TypeExpr2.RefValueType refType) {
    return switch (refType) {
      case BOOLEAN -> {
        final var marker = TypeExpr2.referenceToMarker(Boolean.class);
        yield (buffer, value) -> {
          ZigZagEncoding.putInt(buffer, marker);
          buffer.put((byte) ((Boolean) value ? 1 : 0));
        };
      }
      case BYTE -> {
        final var marker = TypeExpr2.referenceToMarker(Byte.class);
        yield (buffer, value) -> {
          ZigZagEncoding.putInt(buffer, marker);
          buffer.put((Byte) value);
        };
      }
      case SHORT -> {
        final var marker = TypeExpr2.referenceToMarker(Short.class);
        yield (buffer, value) -> {
          ZigZagEncoding.putInt(buffer, marker);
          buffer.putShort((Short) value);
        };
      }
      case CHARACTER -> {
        final var marker = TypeExpr2.referenceToMarker(Character.class);
        yield (buffer, value) -> {
          ZigZagEncoding.putInt(buffer, marker);
          buffer.putChar((Character) value);
        };
      }
      case INTEGER -> {
        final var fixedMarker = TypeExpr2.referenceToMarker(Integer.class);
        final var varMarker = ((TypeExpr2.PrimitiveValueType.IntegerType) TypeExpr2.PrimitiveValueType.INTEGER).varMarker();
        yield (buffer, value) -> {
          final var intValue = (Integer) value;
          if (ZigZagEncoding.sizeOf(intValue) < Integer.BYTES) {
            ZigZagEncoding.putInt(buffer, varMarker);
            ZigZagEncoding.putInt(buffer, intValue);
          } else {
            ZigZagEncoding.putInt(buffer, fixedMarker);
            buffer.putInt(intValue);
          }
        };
      }
      case LONG -> {
        final var fixedMarker = TypeExpr2.referenceToMarker(Long.class);
        final var varMarker = ((TypeExpr2.PrimitiveValueType.LongType) TypeExpr2.PrimitiveValueType.LONG).varMarker();
        yield (buffer, value) -> {
          final var longValue = (Long) value;
          if (ZigZagEncoding.sizeOf(longValue) < Long.BYTES) {
            ZigZagEncoding.putInt(buffer, varMarker);
            ZigZagEncoding.putLong(buffer, longValue);
          } else {
            ZigZagEncoding.putInt(buffer, fixedMarker);
            buffer.putLong(longValue);
          }
        };
      }
      case FLOAT -> {
        final var marker = TypeExpr2.referenceToMarker(Float.class);
        yield (buffer, value) -> {
          ZigZagEncoding.putInt(buffer, marker);
          buffer.putFloat((Float) value);
        };
      }
      case DOUBLE -> {
        final var marker = TypeExpr2.referenceToMarker(Double.class);
        yield (buffer, value) -> {
          ZigZagEncoding.putInt(buffer, marker);
          buffer.putDouble((Double) value);
        };
      }
      case STRING -> {
        final var marker = TypeExpr2.referenceToMarker(String.class);
        yield (buffer, value) -> {
          ZigZagEncoding.putInt(buffer, marker);
          final var bytes = ((String) value).getBytes(java.nio.charset.StandardCharsets.UTF_8);
          ZigZagEncoding.putInt(buffer, bytes.length);
          buffer.put(bytes);
        };
      }
      case LOCAL_DATE, LOCAL_DATE_TIME, CUSTOM, RECORD, ENUM, INTERFACE ->
          throw new UnsupportedOperationException("Type not yet supported in createBuiltInRefWriter: " + refType);
    };
  }

  /// Size a value directly based on its TypeExpr2
  static int sizeValue(
      Object value,
      TypeExpr2 typeExpr,
      Collection<SerdeHandler> customHandlers,
      SizerResolver typeSizerResolver
  ) {
    return switch (typeExpr) {
      case TypeExpr2.PrimitiveValueNode(var type, var javaType) -> {
        throw new IllegalStateException("Primitives cannot be in Optional, should be boxed: " + javaType);
      }
      case TypeExpr2.RefValueNode(var refType, var javaType, var marker) -> {
        if (refType == TypeExpr2.RefValueType.CUSTOM) {
          SerdeHandler handler = customHandlers.stream()
              .filter(h -> h.valueBasedLike().equals(javaType))
              .findFirst()
              .orElseThrow(() -> new IllegalStateException("No handler for custom type: " + javaType));
          yield ZigZagEncoding.sizeOf(handler.marker()) + handler.sizer().applyAsInt(value);
        } else if (refType == TypeExpr2.RefValueType.RECORD || refType == TypeExpr2.RefValueType.ENUM || refType == TypeExpr2.RefValueType.INTERFACE) {
          Sizer sizer = typeSizerResolver.apply((Class<?>) javaType);
          yield sizer.sizeOf(value);
        } else {
          yield sizeBuiltInRefValue(value, refType);
        }
      }
      case TypeExpr2.OptionalNode(var wrappedType) -> {
        if (value instanceof java.util.Optional<?> nested) {
          if (nested.isEmpty()) {
            yield ZigZagEncoding.sizeOf(TypeExpr2.ContainerType.OPTIONAL_EMPTY.marker());
          } else {
            yield ZigZagEncoding.sizeOf(TypeExpr2.ContainerType.OPTIONAL_OF.marker()) +
                sizeValue(nested.get(), wrappedType, customHandlers, typeSizerResolver);
          }
        } else {
          throw new IllegalStateException("Expected Optional but got: " + value.getClass());
        }
      }
      case TypeExpr2.ArrayNode arrayNode ->
          throw new UnsupportedOperationException("Array not yet implemented in sizeValue");
      case TypeExpr2.ListNode(var elementType) -> {
        if (value instanceof java.util.List<?> list) {
          int totalSize = ZigZagEncoding.sizeOf(list.size());
          for (Object item : list) {
            totalSize += Byte.BYTES; // For the null marker
            if (item != null) {
              totalSize += sizeValue(item, elementType, customHandlers, typeSizerResolver);
            }
          }
          yield totalSize;
        } else {
          throw new IllegalStateException("Expected List but got: " + value.getClass());
        }
      }
      case TypeExpr2.MapNode mapNode -> throw new UnsupportedOperationException("Map not yet implemented in sizeValue");
    };
  }

  /// Size built-in reference value where we do a fast worst case estimate without account for zz encoding
  static int sizeBuiltInRefValue(Object value, TypeExpr2.RefValueType refType) {
    return switch (refType) {
      case BOOLEAN, BYTE -> 2 * Byte.BYTES;
      case SHORT -> Byte.BYTES + Short.BYTES;
      case CHARACTER -> Byte.BYTES + Character.BYTES;
      case INTEGER -> Byte.BYTES + Integer.BYTES;
      case LONG -> Byte.BYTES + Long.BYTES;
      case FLOAT -> Byte.BYTES + Float.BYTES;
      case DOUBLE -> Byte.BYTES + Double.BYTES;
      case STRING -> {
        String str = (String) value;
        byte[] bytes = str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        yield 2 * Integer.BYTES + +bytes.length;
      }
      case LOCAL_DATE, LOCAL_DATE_TIME, CUSTOM, RECORD, ENUM, INTERFACE ->
          throw new UnsupportedOperationException("Type not yet supported in sizeBuiltInRefValue: " + refType);
    };
  }

  /// Empty enum to seal the interface
  enum Nothing implements Companion2 {}
}
