// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.jetbrains.annotations.NotNull;

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

interface SizerResolver extends
    Function<Class<?>, Sizer> {
  // For this simple case, the resolvers can be simple lambdas that throw
  // as they shouldn't be called for a trivial record with only primitive components.
  SizerResolver throwsSizerResolver = type -> {
    throw new AssertionError("Sizer throwsSizerResolver should not be reachable.");
  };
}

interface WriterResolver extends
    Function<Class<?>, Writer> {
  WriterResolver throwsWriterResolver = type -> {
    throw new AssertionError("Writer throwsWriterResolver should not be reachable.");
  };

  default Writer resolveWriter(Class<?> type) {
    return apply(type);
  }
}

interface ReaderResolver extends
    Function<Long, Reader> {
  ReaderResolver throwsReaderResolver = type -> {
    throw new AssertionError("Reader throwsReaderResolver should not be reachable.");
  };
}

/// Companion for TypeExpr2-based component serde building
@SuppressWarnings("auxiliaryclass")
sealed interface Companion2 permits Companion2.Nothing {

  /// Build ComponentSerde array for a record type using TypeExpr2 AST
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
      Collection<SerdeHandler> customHandlers,
      WriterResolver typeWriterResolver
  ) {
    return switch (typeExpr) {
      case TypeExpr2.PrimitiveValueNode(var type, var ignored1) -> createPrimitiveWriter(type, getter);

      case TypeExpr2.RefValueNode(var refType, var javaType, var ignored) -> {
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

      case TypeExpr2.ArrayNode(var elementNode, var ignored1) ->
          createArrayWriter(elementNode, getter, customHandlers, typeWriterResolver);
      case TypeExpr2.PrimitiveArrayNode(var primitiveType, var ignored) ->
          createPrimitiveArrayWriter(primitiveType, getter);
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
      case TypeExpr2.PrimitiveValueNode(final var type, final var ignored) -> createPrimitiveReader(type);

      case TypeExpr2.RefValueNode(final var refType, final var javaType, final var ignored) -> {
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

      case TypeExpr2.ArrayNode(var elementNode, var componentType) ->
          createArrayReader(elementNode, componentType, customHandlers, typeReaderResolver);
      case TypeExpr2.PrimitiveArrayNode(var primitiveType, var arrayType) ->
          createPrimitiveArrayReader(primitiveType, arrayType);
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
      Collection<SerdeHandler> customHandlers,
      ReaderResolver typeReaderResolver
  ) {
    return switch (typeExpr) {
      case TypeExpr2.PrimitiveValueNode ignored -> // Primitives cannot be null, so no null check needed
          createValueReaderWithoutNullCheck(typeExpr, customHandlers, typeReaderResolver);

      case TypeExpr2.RefValueNode(final var ignored, final var ignored1, final var ignored2) -> {
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

      case TypeExpr2.ArrayNode(var elementNode, var componentType) ->
          createArrayReader(elementNode, componentType, customHandlers, typeReaderResolver);
      case TypeExpr2.PrimitiveArrayNode(var primitiveType, var arrayType) ->
          createPrimitiveArrayReader(primitiveType, arrayType);
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
      Collection<SerdeHandler> customHandlers,
      SizerResolver typeSizerResolver
  ) {
    return switch (typeExpr) {
      case TypeExpr2.PrimitiveValueNode(var type, var ignored) -> createPrimitiveSizer(type);

      case TypeExpr2.RefValueNode(var refType, var javaType, var ignored) -> {
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

      case TypeExpr2.ArrayNode(var elementNode, var ignored1) ->
          createArraySizer(elementNode, getter, customHandlers, typeSizerResolver);
      case TypeExpr2.PrimitiveArrayNode(var primitiveType, var ignored) ->
          createPrimitiveArraySizer(primitiveType, getter);
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
    // Switch at meta-value time, yield focused function
    return switch (type) {
      case TypeExpr2.PrimitiveValueType.SimplePrimitive(var name, var ignored) -> switch (name) {
        case "BOOLEAN" -> (buffer, obj) -> {
          try {
            buffer.put((byte) ((boolean) getter.invoke(obj) ? 1 : 0));
          } catch (Throwable e) {
            throw new IllegalStateException("Failed to write BOOLEAN", e);
          }
        };
        case "BYTE" -> (buffer, obj) -> {
          try {
            buffer.put((byte) getter.invoke(obj));
          } catch (Throwable e) {
            throw new IllegalStateException("Failed to write BYTE", e);
          }
        };
        case "SHORT" -> (buffer, obj) -> {
          try {
            buffer.putShort((short) getter.invoke(obj));
          } catch (Throwable e) {
            throw new IllegalStateException("Failed to write SHORT", e);
          }
        };
        case "CHARACTER" -> (buffer, obj) -> {
          try {
            buffer.putChar((char) getter.invoke(obj));
          } catch (Throwable e) {
            throw new IllegalStateException("Failed to write CHARACTER", e);
          }
        };
        case "FLOAT" -> (buffer, obj) -> {
          try {
            buffer.putFloat((float) getter.invoke(obj));
          } catch (Throwable e) {
            throw new IllegalStateException("Failed to write FLOAT", e);
          }
        };
        case "DOUBLE" -> (buffer, obj) -> {
          try {
            buffer.putDouble((double) getter.invoke(obj));
          } catch (Throwable e) {
            throw new IllegalStateException("Failed to write DOUBLE", e);
          }
        };
        default -> throw new IllegalStateException("Unknown primitive type: " + name);
      };
      case TypeExpr2.PrimitiveValueType.IntegerType(int fixedMarker, int varMarker) -> (buffer, obj) -> {
        try {
          int result = (int) getter.invoke(obj);
          int zigzagSize = ZigZagEncoding.sizeOf(result);
          if (zigzagSize < Integer.BYTES) {
            ZigZagEncoding.putInt(buffer, varMarker);
            ZigZagEncoding.putInt(buffer, result);
          } else {
            ZigZagEncoding.putInt(buffer, fixedMarker);
            buffer.putInt(result);
          }
        } catch (Throwable e) {
          throw new IllegalStateException("Failed to write INTEGER", e);
        }
      };
      case TypeExpr2.PrimitiveValueType.LongType(int fixedMarker, int varMarker) -> (buffer, obj) -> {
        try {
          long result = (long) getter.invoke(obj);
          int zigzagSize = ZigZagEncoding.sizeOf(result);
          if (zigzagSize < Long.BYTES) {
            ZigZagEncoding.putInt(buffer, varMarker);
            ZigZagEncoding.putLong(buffer, result);
          } else {
            ZigZagEncoding.putInt(buffer, fixedMarker);
            buffer.putLong(result);
          }
        } catch (Throwable e) {
          throw new IllegalStateException("Failed to write LONG", e);
        }
      };
    };
  }

  /// Create reader for primitive types
  static Reader createPrimitiveReader(TypeExpr2.PrimitiveValueType type) {
    // Switch at meta-value time, yield focused function
    return switch (type) {
      case TypeExpr2.PrimitiveValueType.SimplePrimitive(var name, var ignored) -> switch (name) {
        case "BOOLEAN" -> buffer -> buffer.get() != 0;
        case "BYTE" -> buffer -> buffer.get();
        case "SHORT" -> buffer -> buffer.getShort();
        case "CHARACTER" -> buffer -> buffer.getChar();
        case "FLOAT" -> buffer -> buffer.getFloat();
        case "DOUBLE" -> buffer -> buffer.getDouble();
        default -> throw new IllegalStateException("Unknown primitive type: " + name);
      };
      case TypeExpr2.PrimitiveValueType.IntegerType(int fixedMarker, int varMarker) -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker == fixedMarker) {
          return buffer.getInt();
        } else if (marker == varMarker) {
          return ZigZagEncoding.getInt(buffer);
        } else {
          throw new IllegalStateException("Expected INTEGER marker " + fixedMarker + " or " + varMarker + ", got " + marker);
        }
      };
      case TypeExpr2.PrimitiveValueType.LongType(int fixedMarker, int varMarker) -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker == fixedMarker) {
          return buffer.getLong();
        } else if (marker == varMarker) {
          return ZigZagEncoding.getLong(buffer);
        } else {
          throw new IllegalStateException("Expected LONG marker " + fixedMarker + " or " + varMarker + ", got " + marker);
        }
      };
    };
  }

  /// Create sizer for primitive types
  static Sizer createPrimitiveSizer(TypeExpr2.PrimitiveValueType type) {
    // Switch at meta-value time, yield focused function
    return switch (type) {
      case TypeExpr2.PrimitiveValueType.SimplePrimitive(var name, var ignored1) -> switch (name) {
        case "BOOLEAN", "BYTE" -> o -> Byte.BYTES;
        case "SHORT", "CHARACTER" -> o -> Short.BYTES;
        case "FLOAT" -> o -> Float.BYTES;
        case "DOUBLE" -> o -> Double.BYTES;
        default -> throw new IllegalStateException("Unknown primitive type: " + name);
      };
      case TypeExpr2.PrimitiveValueType.IntegerType(int fixedMarker, int varMarker) -> o ->
          // Worst case: marker + 4 bytes data
          ZigZagEncoding.sizeOf(fixedMarker) + Integer.BYTES;
      case TypeExpr2.PrimitiveValueType.LongType(int fixedMarker, int varMarker) -> o ->
          // Worst case: marker + 8 bytes data
          ZigZagEncoding.sizeOf(fixedMarker) + Long.BYTES;
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
  static Sizer createCustomSizer(MethodHandle getter, SerdeHandler handler) {
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
        case RECORD, ENUM, INTERFACE -> // For user types, we don't write a marker. We delegate to the resolver,
          // which is responsible for writing the 'long' signature.
            typeWriterResolver.resolveWriter(javaType).accept(buffer, value);
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
      Class<?> ignoredJavaType,
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
    return extractAndDelegate((value) -> switch (refType) {
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
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
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

  /// Create writer for Array types
  static Writer createArrayWriter(
      TypeExpr2 elementNode,
      MethodHandle getter,
      Collection<SerdeHandler> customHandlers,
      WriterResolver typeWriterResolver
  ) {
    return (buffer, record) -> {
      try {
        Object[] array = (Object[]) getter.invoke(record);
        if (array == null) {
          LOGGER.fine(() -> "Writing null array marker at position " + buffer.position());
          ZigZagEncoding.putInt(buffer, -1); // Use -1 to indicate a null array
        } else {
          LOGGER.fine(() -> "Writing array of size: " + array.length + " at position " + buffer.position());
          ZigZagEncoding.putInt(buffer, array.length);
          for (int i = 0; i < array.length; i++) {
            Object item = array[i];
            final int index = i;
            // Write array item with null handling
            if (item == null) {
              LOGGER.finer(() -> "Writing NULL_MARKER for array item " + index + " at position " + buffer.position());
              buffer.put(NULL_MARKER);
            } else {
              LOGGER.finer(() -> "Writing NOT_NULL_MARKER for array item " + index + " (" + item + ") at position " + buffer.position());
              buffer.put(NOT_NULL_MARKER);
              writeValue(buffer, item, elementNode, customHandlers, typeWriterResolver);
            }
          }
        }
      } catch (Throwable e) {
        throw new IllegalStateException("Failed to write Array", e);
      }
    };
  }

  /// Create reader for Array types
  static Reader createArrayReader(
      TypeExpr2 elementNode,
      Class<?> componentType,
      Collection<SerdeHandler> customHandlers,
      ReaderResolver typeReaderResolver
  ) {
    final var elementReader = createComponentReader(elementNode, customHandlers, typeReaderResolver);
    return buffer -> {
      final int positionBefore = buffer.position();
      final int size = ZigZagEncoding.getInt(buffer);
      LOGGER.fine(() -> "Reading array size: " + size + " at position " + positionBefore);
      if (size == -1) {
        LOGGER.fine(() -> "Read null array marker, returning null");
        return null;
      }
      Object[] array = (Object[]) java.lang.reflect.Array.newInstance(componentType, size);
      LOGGER.fine(() -> "Reading " + size + " elements of type " + componentType.getSimpleName());
      for (int i = 0; i < size; i++) {
        final int index = i;
        LOGGER.finer(() -> "Reading array element " + index + " at position " + buffer.position());
        array[i] = elementReader.apply(buffer);
        LOGGER.finer(() -> "Read array element " + index + ": " + array[index]);
      }
      return array;
    };
  }

  /// Create sizer for Array types
  static Sizer createArraySizer(
      TypeExpr2 elementNode,
      MethodHandle getter,
      Collection<SerdeHandler> customHandlers,
      SizerResolver typeSizerResolver
  ) {
    return record -> {
      try {
        Object[] array = (Object[]) getter.invoke(record);
        if (array == null) {
          return ZigZagEncoding.sizeOf(-1);
        }
        int totalSize = ZigZagEncoding.sizeOf(array.length);
        for (Object item : array) {
          totalSize += Byte.BYTES; // For the null marker
          if (item != null) {
            totalSize += sizeValue(item, elementNode, customHandlers, typeSizerResolver);
          }
        }
        int finalTotalSize = totalSize;
        LOGGER.fine(() -> "Sizing array of length " + array.length + ", total size: " + finalTotalSize);
        return totalSize;
      } catch (Throwable e) {
        throw new IllegalStateException("Failed to size Array", e);
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

  /// Create writer for primitive array types
  static Writer createPrimitiveArrayWriter(TypeExpr2.PrimitiveValueType primitiveType, MethodHandle getter) {
    final BiConsumer<ByteBuffer, Object> valueWriter = buildPrimitiveArrayWriterInner(primitiveType);
    return (buffer, record) -> {
      try {
        Object array = getter.invoke(record);
        if (array == null) {
          ZigZagEncoding.putInt(buffer, -1); // Null array marker
        } else {
          valueWriter.accept(buffer, array);
        }
      } catch (Throwable e) {
        throw new IllegalStateException("Failed to write primitive array", e);
      }
    };
  }

  static @NotNull BiConsumer<ByteBuffer, Object> buildPrimitiveArrayWriterInner(TypeExpr2.PrimitiveValueType primitiveType) {
    return switch (primitiveType) {
      case TypeExpr2.PrimitiveValueType.SimplePrimitive(var name, var ignored) -> switch (name) {
        case "BOOLEAN" -> (buffer, array) -> {
          boolean[] booleans = (boolean[]) array;
          ZigZagEncoding.putInt(buffer, TypeExpr2.primitiveToMarker(boolean.class));
          ZigZagEncoding.putInt(buffer, booleans.length);
          if (booleans.length > 0) {
            java.util.BitSet bitSet = new java.util.BitSet(booleans.length);
            for (int i = 0; i < booleans.length; i++) {
              if (booleans[i]) {
                bitSet.set(i);
              }
            }
            byte[] bytes = bitSet.toByteArray();
            ZigZagEncoding.putInt(buffer, bytes.length);
            buffer.put(bytes);
          } else {
            ZigZagEncoding.putInt(buffer, 0);
          }
        };
        case "BYTE" -> (buffer, array) -> {
          byte[] bytes = (byte[]) array;
          ZigZagEncoding.putInt(buffer, TypeExpr2.primitiveToMarker(byte.class));
          ZigZagEncoding.putInt(buffer, bytes.length);
          buffer.put(bytes);
        };
        case "SHORT" -> (buffer, array) -> {
          short[] shorts = (short[]) array;
          ZigZagEncoding.putInt(buffer, TypeExpr2.primitiveToMarker(short.class));
          ZigZagEncoding.putInt(buffer, shorts.length);
          for (short s : shorts) buffer.putShort(s);
        };
        case "CHARACTER" -> (buffer, array) -> {
          char[] chars = (char[]) array;
          ZigZagEncoding.putInt(buffer, TypeExpr2.primitiveToMarker(char.class));
          ZigZagEncoding.putInt(buffer, chars.length);
          for (char c : chars) buffer.putChar(c);
        };
        case "FLOAT" -> (buffer, array) -> {
          float[] floats = (float[]) array;
          ZigZagEncoding.putInt(buffer, TypeExpr2.primitiveToMarker(float.class));
          ZigZagEncoding.putInt(buffer, floats.length);
          for (float f : floats) buffer.putFloat(f);
        };
        case "DOUBLE" -> (buffer, array) -> {
          double[] doubles = (double[]) array;
          ZigZagEncoding.putInt(buffer, TypeExpr2.primitiveToMarker(double.class));
          ZigZagEncoding.putInt(buffer, doubles.length);
          for (double d : doubles) buffer.putDouble(d);
        };
        default -> throw new IllegalStateException("Unknown primitive type: " + name);
      };
      case TypeExpr2.PrimitiveValueType.IntegerType(int fixedMarker, int varMarker) -> (buffer, array) -> {
        int[] ints = (int[]) array;
        // Simplified sampling: if any value is large, use fixed encoding for all.
        boolean useVar = Arrays.stream(ints).allMatch(i -> ZigZagEncoding.sizeOf(i) < Integer.BYTES);
        if (useVar) {
          ZigZagEncoding.putInt(buffer, varMarker);
          ZigZagEncoding.putInt(buffer, ints.length);
          for (int i : ints) ZigZagEncoding.putInt(buffer, i);
        } else {
          ZigZagEncoding.putInt(buffer, fixedMarker);
          ZigZagEncoding.putInt(buffer, ints.length);
          for (int i : ints) buffer.putInt(i);
        }
      };
      case TypeExpr2.PrimitiveValueType.LongType(int fixedMarker, int varMarker) -> (buffer, array) -> {
        long[] longs = (long[]) array;
        boolean useVar = Arrays.stream(longs).allMatch(l -> ZigZagEncoding.sizeOf(l) < Long.BYTES);
        if (useVar) {
          ZigZagEncoding.putInt(buffer, varMarker);
          ZigZagEncoding.putInt(buffer, longs.length);
          for (long l : longs) ZigZagEncoding.putLong(buffer, l);
        } else {
          ZigZagEncoding.putInt(buffer, fixedMarker);
          ZigZagEncoding.putInt(buffer, longs.length);
          for (long l : longs) buffer.putLong(l);
        }
      };
    };
  }

  /// Create reader for primitive array types
  static Reader createPrimitiveArrayReader(TypeExpr2.PrimitiveValueType primitiveType, Class<?> ignoredArrayType) {
    final Function<ByteBuffer, Object> valueReader = buildPrimitiveArrayReaderInner(primitiveType);
    return buffer -> {
      int size = ZigZagEncoding.getInt(buffer);
      if (size == -1) {
        return null;
      }
      // Rewind to read the marker again inside the value reader
      buffer.position(buffer.position() - ZigZagEncoding.sizeOf(size));
      return valueReader.apply(buffer);
    };
  }

  static @NotNull Function<ByteBuffer, Object> buildPrimitiveArrayReaderInner(TypeExpr2.PrimitiveValueType primitiveType) {
    return switch (primitiveType) {
      case TypeExpr2.PrimitiveValueType.SimplePrimitive(var name, var marker) -> switch (name) {
        case "BOOLEAN" -> buffer -> {
          int typeMarker = ZigZagEncoding.getInt(buffer);
          assert typeMarker == marker : "Expected BOOLEAN array marker";
          int arrayLength = ZigZagEncoding.getInt(buffer);
          boolean[] booleans = new boolean[arrayLength];
          if (arrayLength > 0) {
            int bytesLength = ZigZagEncoding.getInt(buffer);
            byte[] bytes = new byte[bytesLength];
            buffer.get(bytes);
            java.util.BitSet bitSet = java.util.BitSet.valueOf(bytes);
            for (int i = 0; i < arrayLength; i++) booleans[i] = bitSet.get(i);
          }
          return booleans;
        };
        case "BYTE" -> buffer -> {
          int typeMarker = ZigZagEncoding.getInt(buffer);
          assert typeMarker == marker : "Expected BYTE array marker";
          int length = ZigZagEncoding.getInt(buffer);
          byte[] bytes = new byte[length];
          buffer.get(bytes);
          return bytes;
        };
        case "SHORT" -> buffer -> {
          int typeMarker = ZigZagEncoding.getInt(buffer);
          assert typeMarker == marker : "Expected SHORT array marker";
          int length = ZigZagEncoding.getInt(buffer);
          short[] shorts = new short[length];
          for (int i = 0; i < length; i++) shorts[i] = buffer.getShort();
          return shorts;
        };
        case "CHARACTER" -> buffer -> {
          int typeMarker = ZigZagEncoding.getInt(buffer);
          assert typeMarker == marker : "Expected CHARACTER array marker";
          int length = ZigZagEncoding.getInt(buffer);
          char[] chars = new char[length];
          for (int i = 0; i < length; i++) chars[i] = buffer.getChar();
          return chars;
        };
        case "FLOAT" -> buffer -> {
          int typeMarker = ZigZagEncoding.getInt(buffer);
          assert typeMarker == marker : "Expected FLOAT array marker";
          int length = ZigZagEncoding.getInt(buffer);
          float[] floats = new float[length];
          for (int i = 0; i < length; i++) floats[i] = buffer.getFloat();
          return floats;
        };
        case "DOUBLE" -> buffer -> {
          int typeMarker = ZigZagEncoding.getInt(buffer);
          assert typeMarker == marker : "Expected DOUBLE array marker";
          int length = ZigZagEncoding.getInt(buffer);
          double[] doubles = new double[length];
          for (int i = 0; i < length; i++) doubles[i] = buffer.getDouble();
          return doubles;
        };
        default -> throw new IllegalStateException("Unknown primitive type: " + name);
      };
      case TypeExpr2.PrimitiveValueType.IntegerType(int ignored, int varMarker) -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        int length = ZigZagEncoding.getInt(buffer);
        int[] ints = new int[length];
        if (marker == varMarker) {
          for (int i = 0; i < length; i++) ints[i] = ZigZagEncoding.getInt(buffer);
        } else {
          for (int i = 0; i < length; i++) ints[i] = buffer.getInt();
        }
        return ints;
      };
      case TypeExpr2.PrimitiveValueType.LongType(int ignored, int varMarker) -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        int length = ZigZagEncoding.getInt(buffer);
        long[] longs = new long[length];
        if (marker == varMarker) {
          for (int i = 0; i < length; i++) longs[i] = ZigZagEncoding.getLong(buffer);
        } else {
          for (int i = 0; i < length; i++) longs[i] = buffer.getLong();
        }
        return longs;
      };
    };
  }

  /// Create sizer for primitive array types
  static Sizer createPrimitiveArraySizer(TypeExpr2.PrimitiveValueType primitiveType, MethodHandle getter) {
    final ToIntFunction<Object> valueSizer = buildPrimitiveArraySizerInner(primitiveType);
    return record -> {
      try {
        Object array = getter.invoke(record);
        if (array == null) {
          return ZigZagEncoding.sizeOf(-1);
        }
        return valueSizer.applyAsInt(array);
      } catch (Throwable e) {
        throw new IllegalStateException("Failed to size primitive array", e);
      }
    };
  }

  static @NotNull ToIntFunction<Object> buildPrimitiveArraySizerInner(TypeExpr2.PrimitiveValueType primitiveType) {
    return switch (primitiveType) {
      case TypeExpr2.PrimitiveValueType.SimplePrimitive(var name, var marker) -> switch (name) {
        case "BOOLEAN" -> array -> {
          boolean[] booleans = (boolean[]) array;
          java.util.BitSet bitSet = new java.util.BitSet(booleans.length);
          for (int i = 0; i < booleans.length; i++) if (booleans[i]) bitSet.set(i);
          byte[] bytes = bitSet.toByteArray();
          return ZigZagEncoding.sizeOf(marker) + ZigZagEncoding.sizeOf(booleans.length) + ZigZagEncoding.sizeOf(bytes.length) + bytes.length;
        };
        case "BYTE" ->
            array -> ZigZagEncoding.sizeOf(marker) + ZigZagEncoding.sizeOf(((byte[]) array).length) + ((byte[]) array).length;
        case "SHORT" ->
            array -> ZigZagEncoding.sizeOf(marker) + ZigZagEncoding.sizeOf(((short[]) array).length) + ((short[]) array).length * Short.BYTES;
        case "CHARACTER" ->
            array -> ZigZagEncoding.sizeOf(marker) + ZigZagEncoding.sizeOf(((char[]) array).length) + ((char[]) array).length * Character.BYTES;
        case "FLOAT" ->
            array -> ZigZagEncoding.sizeOf(marker) + ZigZagEncoding.sizeOf(((float[]) array).length) + ((float[]) array).length * Float.BYTES;
        case "DOUBLE" ->
            array -> ZigZagEncoding.sizeOf(marker) + ZigZagEncoding.sizeOf(((double[]) array).length) + ((double[]) array).length * Double.BYTES;
        default -> throw new IllegalStateException("Unknown primitive type: " + name);
      };
      case TypeExpr2.PrimitiveValueType.IntegerType(int fixedMarker, int ignored) -> array -> {
        int[] ints = (int[]) array;
        // Worst-case sizing
        return ZigZagEncoding.sizeOf(fixedMarker) + ZigZagEncoding.sizeOf(ints.length) + ints.length * Integer.BYTES;
      };
      case TypeExpr2.PrimitiveValueType.LongType(int fixedMarker, int ignored) -> array -> {
        long[] longs = (long[]) array;
        // Worst-case sizing
        return ZigZagEncoding.sizeOf(fixedMarker) + ZigZagEncoding.sizeOf(longs.length) + longs.length * Long.BYTES;
      };
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
      case TypeExpr2.PrimitiveValueNode(
          var ignored, var javaType
      ) -> // For primitive types, we can't have them in Optional, this should be a RefValueNode
          throw new IllegalStateException("Primitives cannot be in Optional, should be boxed: " + javaType);
      case TypeExpr2.RefValueNode(var refType, var javaType, var ignored) -> {
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
      case TypeExpr2.ArrayNode(var elementNode, var ignored) -> {
        Object[] array = (Object[]) value;
        if (array == null) {
          ZigZagEncoding.putInt(buffer, -1); // Use -1 to indicate a null array
        } else {
          LOGGER.fine(() -> "Writing array of size: " + array.length + " at position: " + buffer.position());
          ZigZagEncoding.putInt(buffer, array.length);
          for (Object item : array) {
            if (item == null) {
              buffer.put(NULL_MARKER);
            } else {
              buffer.put(NOT_NULL_MARKER);
              writeValue(buffer, item, elementNode, customHandlers, typeWriterResolver);
            }
          }
        }
      }
      case TypeExpr2.PrimitiveArrayNode(var primitiveType, var ignored) ->
          buildPrimitiveArrayWriterInner(primitiveType).accept(buffer, value);
      case TypeExpr2.ListNode ignored ->
          throw new UnsupportedOperationException("List not yet implemented in writeValue");
      case TypeExpr2.MapNode ignored ->
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
      case TypeExpr2.PrimitiveValueNode(var ignored, var javaType) ->
          throw new IllegalStateException("Primitives cannot be in Optional, should be boxed: " + javaType);
      case TypeExpr2.RefValueNode(var refType, var javaType, var ignored) -> {
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
      case TypeExpr2.ArrayNode(var elementNode, var ignored1) -> {
        Object[] array = (Object[]) value;
        if (array == null) {
          yield ZigZagEncoding.sizeOf(-1);
        }
        int itemsSize = Arrays.stream(array)
            .mapToInt(item -> {
              int itemSize = Byte.BYTES; // For the null marker
              if (item != null) {
                itemSize += sizeValue(item, elementNode, customHandlers, typeSizerResolver);
              }
              return itemSize;
            })
            .sum();
        yield ZigZagEncoding.sizeOf(array.length) + itemsSize;
      }
      case TypeExpr2.PrimitiveArrayNode(var primitiveType, var ignored) ->
          buildPrimitiveArraySizerInner(primitiveType).applyAsInt(value);
      case TypeExpr2.ListNode(var elementType) -> {
        if (value instanceof java.util.List<?> list) {
          int itemsSize = list.stream()
              .mapToInt(item -> {
                int itemSize = Byte.BYTES; // For the null marker
                if (item != null) {
                  itemSize += sizeValue(item, elementType, customHandlers, typeSizerResolver);
                }
                return itemSize;
              })
              .sum();
          yield ZigZagEncoding.sizeOf(list.size()) + itemsSize;
        } else {
          throw new IllegalStateException("Expected List but got: " + value.getClass());
        }
      }
      case TypeExpr2.MapNode ignored -> throw new UnsupportedOperationException("Map not yet implemented in sizeValue");
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
        yield 2 * Integer.BYTES + bytes.length;
      }
      case LOCAL_DATE, LOCAL_DATE_TIME, CUSTOM, RECORD, ENUM, INTERFACE ->
          throw new UnsupportedOperationException("Type not yet supported in sizeBuiltInRefValue: " + refType);
    };
  }

  /// Empty enum to seal the interface
  enum Nothing implements Companion2 {}
}
