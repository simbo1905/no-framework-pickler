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
      Function<Long, Function<ByteBuffer, Object>> typeReaderResolver
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
      Function<Long, Function<ByteBuffer, Object>> typeReaderResolver
  ) {
    return switch (typeExpr) {
      case TypeExpr2.PrimitiveValueNode(var type, var javaType) -> createPrimitiveReader(type);

      case TypeExpr2.RefValueNode(var refType, var javaType, var marker) -> {
        final Function<ByteBuffer, Object> valueReader;
        if (refType == TypeExpr2.RefValueType.CUSTOM) {
          SerdeHandler handler = customHandlers.stream()
              .filter(h -> h.valueBasedLike().equals(javaType))
              .findFirst()
              .orElseThrow(() -> new IllegalStateException("No handler for custom type: " + javaType));
          valueReader = buffer -> {
            int typeMarker = ZigZagEncoding.getInt(buffer);
            assert typeMarker == handler.marker() : "Expected marker " + handler.marker() + " but got " + typeMarker;
            return handler.reader().apply(buffer);
          };
        } else {
          valueReader = createRefValueReader(refType, (Class<?>) javaType, typeReaderResolver);
        }

        // Wrap the value reader with a null check
        yield buffer -> {
          byte nullMarker = buffer.get();
          if (nullMarker == NULL_MARKER) {
            return null;
          } else if (nullMarker == NOT_NULL_MARKER) {
            return valueReader.apply(buffer);
          } else {
            throw new IllegalStateException("Invalid null marker: " + nullMarker);
          }
        };
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
  static Function<ByteBuffer, Object> createPrimitiveReader(TypeExpr2.PrimitiveValueType type) {
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

  /// Create sizer for primitive types
  static ToIntFunction<Object> createPrimitiveSizer(TypeExpr2.PrimitiveValueType type) {
    // Primitive types don't need the record object, just return worst-case sizes
    return obj -> switch (type) {
      case TypeExpr2.PrimitiveValueType.SimplePrimitive(var name, var marker) -> {
        yield switch (name) {
          case "BOOLEAN", "BYTE" -> Byte.BYTES;
          case "SHORT", "CHARACTER" -> Short.BYTES;
          case "FLOAT" -> Float.BYTES;
          case "DOUBLE" -> Double.BYTES;
          default -> throw new IllegalStateException("Unknown primitive type: " + name);
        };
      }
      case TypeExpr2.PrimitiveValueType.IntegerType ignored ->
        // Worst case: 1 byte marker + 4 bytes data
          ZigZagEncoding.sizeOf(-6) + Integer.BYTES;
      case TypeExpr2.PrimitiveValueType.LongType ignored ->
        // Worst case: 1 byte marker + 8 bytes data
          ZigZagEncoding.sizeOf(-8) + Long.BYTES;
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
    // Use extractAndDelegate for null handling. The returned delegate handles the non-null value.
    return extractAndDelegate((buffer, value) -> {
      switch (refType) {
        case RECORD, ENUM, INTERFACE -> {
          // For user types, we don't write a marker. We delegate to the resolver,
          // which is responsible for writing the 'long' signature.
          BiConsumer<ByteBuffer, Object> writer = typeWriterResolver.apply(javaType);
          writer.accept(buffer, value);
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
              if (intValue >= -1 && intValue <= 125) { // Tiny int optimization
                ZigZagEncoding.putInt(buffer, TypeExpr2.referenceToMarker(Integer.class));
                buffer.put((byte) intValue);
              } else {
                ZigZagEncoding.putInt(buffer, ((TypeExpr2.PrimitiveValueType.IntegerType) TypeExpr2.PrimitiveValueType.INTEGER).varMarker()); // Varint marker
                ZigZagEncoding.putInt(buffer, intValue);
              }
            }
            case LONG -> {
              long longValue = (Long) value;
              if (longValue >= -1 && longValue <= 125) { // Tiny long optimization
                ZigZagEncoding.putInt(buffer, TypeExpr2.referenceToMarker(Long.class));
                buffer.put((byte) longValue);
              } else {
                ZigZagEncoding.putInt(buffer, ((TypeExpr2.PrimitiveValueType.LongType) TypeExpr2.PrimitiveValueType.LONG).varMarker()); // Varlong marker
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
            case LOCAL_DATE, LOCAL_DATE_TIME, CUSTOM ->
                throw new UnsupportedOperationException("Type not yet supported: " + refType);
          }
        }
      }
    }, getter);
  }

  /// Create reader for reference value types
  static Function<ByteBuffer, Object> createRefValueReader(
      TypeExpr2.RefValueType refType,
      Class<?> javaType,
      Function<Long, Function<ByteBuffer, Object>> typeReaderResolver
  ) {
    // This is the "meta-programming time" decision based on the AST.
    // We return a specialized function based on whether it's a user type or not.
    switch (refType) {
      case RECORD, ENUM, INTERFACE:
        // Return a reader specifically for user types.
        // This reader is called AFTER the component's null-marker has been read.
        // It expects a 'long' signature directly from the buffer.
        return buffer -> {
          final long typeSignature = buffer.getLong();
          // The resolver is responsible for handling the object,
          // including the special 0L signature for a null object,
          // though 0L should not appear for a non-null component.
          return typeReaderResolver.apply(typeSignature).apply(buffer);
        };

      default:
        // Return a reader for built-in reference types.
        // This reader is called AFTER the component's null-marker has been read.
        // It expects an 'int' marker before the data.
        return buffer -> {
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
              if (marker == TypeExpr2.referenceToMarker(Integer.class)) { // This is for the tiny int encoding
                yield (int) buffer.get();
              } else if (marker == ((TypeExpr2.PrimitiveValueType.IntegerType) TypeExpr2.PrimitiveValueType.INTEGER).varMarker()) { // This is for the varint encoding
                yield ZigZagEncoding.getInt(buffer);
              } else {
                throw new IllegalStateException("Expected INTEGER marker, got " + marker);
              }
            }
            case LONG -> {
              if (marker == TypeExpr2.referenceToMarker(Long.class)) { // tiny long
                yield (long) buffer.get();
              } else if (marker == ((TypeExpr2.PrimitiveValueType.LongType) TypeExpr2.PrimitiveValueType.LONG).varMarker()) { // varlong
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
            case RECORD, ENUM, INTERFACE -> throw new AssertionError("Should be handled by the outer switch");
            case LOCAL_DATE, LOCAL_DATE_TIME, CUSTOM ->
                throw new UnsupportedOperationException("Type not yet supported: " + refType);
          };
        };
    }
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
          ToIntFunction<Object> sizer = typeSizerResolver.apply(javaType);
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

  /// Empty enum to seal the interface
  enum Nothing implements Companion2 {}
}
