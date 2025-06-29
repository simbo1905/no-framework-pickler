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
import java.lang.reflect.Type;
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

import static io.github.simbo1905.no.framework.Companion.recordClassHierarchy;
import static io.github.simbo1905.no.framework.Companion.writeToWireWitness;
import static io.github.simbo1905.no.framework.ManyPickler.*;

final class RecordPickler<T> implements Pickler<T> {
  public static final String SHA_256 = "SHA-256";
  static final int SAMPLE_SIZE = 32;
  static final int CLASS_SIG_BYTES = Long.BYTES;
  // Global lookup tables indexed by ordinal - the core of the unified architecture
  final Class<?> userType;
  final long typeSignature;    // CLASS_SIG_BYTES SHA256 signatures for backwards compatibility checking
  final MethodHandle recordConstructor;      // Constructor direct method handle
  final MethodHandle[] componentAccessors;    // Accessor direct method handle
  final TypeExpr[] componentTypeExpressions;       // Component type AST structure
  final BiConsumer<ByteBuffer, Object>[] componentWriters;  // writer chain of delegating lambda eliminating use of `switch` on write the hot path
  final Function<ByteBuffer, Object>[] componentReaders;    // reader chain of delegating lambda eliminating use of `switch` on read the hot path
  final ToIntFunction<Object>[] componentSizers; // Sizer lambda
  final Map<Class<?>, Pickler<?>> picklers; // Map of record classes to their picklers
  final Map<Long, Pickler<?>> typeSignatureToPicklerMap;
  final Map<Class<?>, Long> recordClassToTypeSignatureMap;
  final Map<Class<Enum<?>>, Long> enumToTypeSignatureMap;
  final Map<Long, Class<Enum<?>>> typeSignatureToEnumMap;


  public RecordPickler(@NotNull Class<?> userType, final Map<Class<Enum<?>>, Long> enumToTypeSignatureMap) {
    assert userType.isRecord() && enumToTypeSignatureMap != null;
    this.userType = userType;
    this.enumToTypeSignatureMap = enumToTypeSignatureMap;
    // Create the inverse map of type signatures to enum classes
    typeSignatureToEnumMap = enumToTypeSignatureMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

    final Set<Class<?>> recordClassHierarchy = recordClassHierarchy(userType)
        .stream().filter(Class::isRecord).collect(Collectors.toSet());

    LOGGER.fine(() -> "RecordPickler " + userType.getSimpleName() + " record class hierarchy: " + recordClassHierarchy.stream().map(Class::getSimpleName).collect(Collectors.joining(", ")));

    if (recordClassHierarchy.size() > 1) {
      // resolve any nested record type or nested enum types
      final Map<Boolean, List<Class<?>>> userTypesAndOthers =
          recordClassHierarchy.stream()
              .filter(cls -> !userType.equals(cls))
              .collect(Collectors.partitioningBy(
                  cls -> cls.isRecord() || cls.isEnum()
              ));

      LOGGER.fine(() -> "RecordPickler " + userType + " resolve componentPicker for " + userTypesAndOthers.get(Boolean.TRUE).stream().map(Class::getSimpleName)
          .collect(Collectors.joining(", ")) + " followed by type signatures for enums " +
          userTypesAndOthers.get(Boolean.FALSE).stream().map(Class::getSimpleName)
              .collect(Collectors.joining(",")));

      // create or resolve any picklers for records and enums
      picklers = userTypesAndOthers.get(Boolean.TRUE).stream()
          .filter(Class::isRecord)
          .collect(Collectors.toMap(clz -> clz,
              clz ->
                  REGISTRY.computeIfAbsent(clz, c -> resolvePickerNoCache(c, enumToTypeSignatureMap))));

      // Create a map of type signatures to picklers
      this.typeSignatureToPicklerMap = picklers.values().stream().map(pickler -> Map.entry(switch (pickler) {
        case RecordPickler<?> rp -> rp.typeSignature;
        case EmptyRecordPickler<?> erp -> erp.typeSignature;
        default ->
            throw new IllegalArgumentException("Record Pickler " + userType.getSimpleName() + " unexpected pickler type: " + pickler.getClass());
      }, pickler)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      LOGGER.fine(() -> "RecordPickler " + userType + " typeSignatureToPicklerMap contents: " +
          typeSignatureToPicklerMap.entrySet().stream()
              .map(entry -> "0x" + Long.toHexString(entry.getKey()) + " -> " + entry.getValue())
              .collect(Collectors.joining(", ")));

      // Create the inverse map of picklers to type signatures
      this.recordClassToTypeSignatureMap = picklers.entrySet().stream().map(classAndPickler -> Map.entry(classAndPickler.getKey(), switch (classAndPickler.getValue()) {
        case RecordPickler<?> rp -> rp.typeSignature;
        case EmptyRecordPickler<?> erp -> erp.typeSignature;
        default ->
            throw new IllegalArgumentException("Record Pickler " + userType.getSimpleName() + " unexpected pickler type: " + classAndPickler.getValue());
      })).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      // Create a map of enum classes to their type signatures
    } else {
      this.typeSignatureToPicklerMap = Map.of();
      this.recordClassToTypeSignatureMap = Map.of();
      this.picklers = Map.of();
    }

    // Get component accessors and analyze types
    RecordComponent[] components = userType.getRecordComponents();
    assert components != null && components.length > 0 : "Record type must have at least one component: " + userType.getName();
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
      LOGGER.finer(() -> "RecordPickler " + userType.getSimpleName() + " begin building writer/reader/sizer chains for component " + i + " with type " + typeExpr.toTreeString());
      // Build writer, reader, and sizer chains
      componentReaders[i] = buildReaderChainFromAST(typeExpr);
      componentSizers[i] = buildSizerChainFromAST(typeExpr, accessor);
      componentWriters[i] = buildWriterChainFromAST(typeExpr, accessor);
      LOGGER.finer(() -> "RecordPickler " + userType.getSimpleName() + " end writer/reader/sizer chains for component " + i + " with type " + typeExpr.toTreeString());
    });

    LOGGER.fine(() -> "RecordPickler constructor - typeSignatureToPicklerMap contents: " +
        typeSignatureToPicklerMap.entrySet().stream()
            .map(entry -> "0x" + Long.toHexString(entry.getKey()) + " -> " + entry.getValue())
            .collect(Collectors.joining(", ")));

    LOGGER.fine(() -> "RecordPickler constructor - recordClassToTypeSignatureMap contents: " +
        recordClassToTypeSignatureMap.entrySet().stream()
            .map(entry -> entry.getKey().getSimpleName() + " -> 0x" + Long.toHexString(entry.getValue()))
            .collect(Collectors.joining(", ")));

    LOGGER.info(() -> "RecordPickler " + userType.getSimpleName() + " construction complete for " + userType.getSimpleName());
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
      return RecordPickler.hashSignature(input);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  @NotNull Function<ByteBuffer, Object> buildValueReader(TypeExpr.RefValueType valueType) {
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
      case ENUM -> enumValueReader();
      case RECORD -> // Delegate to the recordValueReader for the specific type
          recordValueReader();
      case INTERFACE -> (buffer) -> {
        final int positionBeforeRead = buffer.position();
        long typeSignature = buffer.getLong();
        if (typeSignature == 0L) {
          LOGGER.fine(() -> "RecordPickler " + userType.getSimpleName() + " read 0L typeSignature to returning null for record at position: " + buffer.position());
          return null; // null marker
        }
        if (typeSignature == this.typeSignature) {
          LOGGER.fine(() -> "RecordPickler " + userType.getSimpleName() + " read self type signature 0x" + Long.toHexString(typeSignature) + " at position: " + positionBeforeRead);
          // Fast path for self type signature
          return readFromWire(buffer);
        } else if (typeSignatureToEnumMap.containsKey(typeSignature)) {
          final int ordinal = ZigZagEncoding.getInt(buffer);
          LOGGER.fine(() -> "RecordPickler " + userType.getSimpleName() + " enumValueReader read ordinal: " + ordinal + ", typeSignature: 0x" + Long.toHexString(typeSignature) + " at position: " + positionBeforeRead);
          Class<?> enumClass = typeSignatureToEnumMap.get(typeSignature);
          if (enumClass == null) {
            throw new IllegalStateException("No enum class found for type signature: " + typeSignature);
          }
          Enum<?>[] constants = (Enum<?>[]) enumClass.getEnumConstants();
          if (ordinal < 0 || ordinal >= constants.length) {
            throw new IndexOutOfBoundsException("Invalid enum ordinal: " + ordinal + " for class: " + enumClass.getSimpleName());
          }
          return constants[ordinal];
        } else if (typeSignatureToPicklerMap.containsKey(typeSignature)) {
          Pickler<?> pickler = typeSignatureToPicklerMap.get(typeSignature);
          return switch (pickler) {
            case EmptyRecordPickler<?> erp -> {
              LOGGER.fine(() -> "RecordPickler " + userType.getSimpleName() + " read EmptyRecordPickler singleton for class " + erp.userType.getSimpleName() + " record for type signature 0x" + Long.toHexString(typeSignature) + " at position: " + positionBeforeRead);
              yield erp.singleton;
            }
            case RecordPickler<?> rp -> {
              LOGGER.fine(() -> "RecordPickler " + userType.getSimpleName() + " delegating to RecordReader for " + rp.userType.getSimpleName() + " for type signature 0x" + Long.toHexString(typeSignature) + " at position: " + positionBeforeRead);
              yield rp.readFromWire(buffer);
            }
            default -> throw new AssertionError("Unexpected pickler class: " + pickler.getClass());
          };
        } else {
          LOGGER.fine("Available picker type signatures in map: " + typeSignatureToPicklerMap.keySet().stream()
              .map(sig -> "0x" + Long.toHexString(sig))
              .collect(Collectors.joining(", ")));
          LOGGER.fine("Available enum type signatures in map: " + typeSignatureToEnumMap.keySet().stream()
              .map(sig -> "0x" + Long.toHexString(sig))
              .collect(Collectors.joining(", ")));
          throw new IllegalStateException("RecordPickler " + userType.getSimpleName() + " type signature not found in either map: " + Long.toHexString(typeSignature) + " at position: " + buffer.position());
        }
      };
    };
  }

  @NotNull Function<ByteBuffer, Object> recordValueReader() {
    return (buffer) -> {
      final var positionBeforeRead = buffer.position();
      final long typeSignature = buffer.getLong();
      if (typeSignature == 0L) {
        LOGGER.fine(() -> "RecordPickler " + userType.getSimpleName() + " read 0L typeSignature to returning null for record at position: " + positionBeforeRead);
        return null; // null marker
      }
      if (typeSignature == this.typeSignature) {
        LOGGER.fine(() -> "RecordPickler " + userType.getSimpleName() + " read self type signature 0x" + Long.toHexString(typeSignature) + " at position: " + positionBeforeRead);
        // Fast path for self type signature
        return readFromWire(buffer);
      } else {
        LOGGER.fine(() -> "RecordPickler " + userType.getSimpleName() + " read type signature 0x" + Long.toHexString(typeSignature) + " at position: " + positionBeforeRead);
        assert typeSignatureToPicklerMap.containsKey(typeSignature) : "Type signature not found in map: " + Long.toHexString(typeSignature);
        Pickler<?> pickler = typeSignatureToPicklerMap.get(typeSignature);
        return switch (pickler) {
          case EmptyRecordPickler<?> erp -> {
            LOGGER.fine(() -> "RecordPickler " + userType.getSimpleName() + " read empty record for type signature 0x" + Long.toHexString(typeSignature) + " at position: " + buffer.position());
            yield erp.singleton;
          }
          case RecordPickler<?> rp -> {
            LOGGER.fine(() -> "RecordPickler " + userType.getSimpleName() + " delegating to record " + rp + " for type signature 0x" + Long.toHexString(typeSignature) + " at position: " + buffer.position());
            yield rp.readFromWire(buffer);
          }
          default -> throw new AssertionError("Unexpected pickler class: " + pickler.getClass());
        };
      }
    };
  }

  @NotNull Function<ByteBuffer, Object> enumValueReader() {
    return (buffer) -> {
      final int positionBeforeRead = buffer.position();
      final long typeSignature = buffer.getLong();
      final int ordinal = ZigZagEncoding.getInt(buffer);
      LOGGER.fine(() -> "RecordPickler " + userType.getSimpleName() + " enumValueReader read ordinal: " + ordinal + ", typeSignature: 0x" + Long.toHexString(typeSignature) + " at position: " + positionBeforeRead);
      if (typeSignature == 0L) {
        LOGGER.fine(() -> "RecordPickler " + userType.getSimpleName() + " read 0L typeSignature to returning null for enum at position: " + positionBeforeRead);
        return null; // null marker
      }
      assert typeSignatureToEnumMap.containsKey(typeSignature) : "Type signature not found in map: " + Long.toHexString(typeSignature) + " available signatures: " +
          typeSignatureToEnumMap.keySet().stream()
              .map(sig -> "0x" + Long.toHexString(sig))
              .collect(Collectors.joining(", "));
      Class<?> enumClass = typeSignatureToEnumMap.get(typeSignature);
      if (enumClass == null) {
        throw new IllegalStateException("No enum class found for type signature: " + typeSignature);
      }
      Enum<?>[] constants = (Enum<?>[]) enumClass.getEnumConstants();
      if (ordinal < 0 || ordinal >= constants.length) {
        throw new IndexOutOfBoundsException("Invalid enum ordinal: " + ordinal + " for class: " + enumClass.getSimpleName());
      }
      return constants[ordinal];
    };
  }

  int maxSizeOfRecordComponents(T record) {
    int totalSize = 0;
    for (ToIntFunction<Object> sizer : this.componentSizers) {
      totalSize += sizer.applyAsInt(record);
    }
    return totalSize;
  }

  @Override
  public int serialize(ByteBuffer buffer, T record) {
    Objects.requireNonNull(buffer, "buffer cannot be null");
    Objects.requireNonNull(record, "record cannot be null");
    buffer.order(ByteOrder.BIG_ENDIAN);
    final var clz = record.getClass();
    if (this.userType.isAssignableFrom(clz)) {
      LOGGER.fine(() -> "RecordPickler " + userType.getSimpleName() + " serialize() calling writeToWire for " + clz.getSimpleName() + " at position: " + buffer.position());
      return writeToWire(buffer, record);
    } else {
      throw new IllegalArgumentException("RecordPickler " + userType.getSimpleName() + " expected a record type " + this.userType.getName() + " but got: " + clz.getName());
    }
  }

  int writeToWire(ByteBuffer buffer, T record) { // FIXME we ignore the returned type a lot so split it into an inner
    final var startPosition = buffer.position();
    LOGGER.fine(() -> "RecordPickler " + userType.getSimpleName() + " writeToWire writing type signature 0x" + Long.toHexString(typeSignature) + " for class  " + record.getClass().getSimpleName() + " at position: " + buffer.position());
    buffer.putLong(typeSignature);
    serializeRecordComponents(buffer, record);
    final var totalBytes = buffer.position() - startPosition;
    LOGGER.finer(() -> "RecordPickler " + userType.getSimpleName() + " serialize: completed, totalBytes=" + totalBytes);
    return totalBytes;
  }

  /// Deserialize a record from the given buffer, first checking the type signature.
  @Override
  public T deserialize(ByteBuffer buffer) {
    Objects.requireNonNull(buffer);
    buffer.order(ByteOrder.BIG_ENDIAN);
    final int typeSigPosition = buffer.position();
    // read the type signature first as it is a cryptographic hash of the class name and component metadata and fixed size
    final long incomingSignature = buffer.getLong();
    LOGGER.fine(() -> "RecordPickler " + userType.getSimpleName() + " deserialize() read type signature 0x" + Long.toHexString(incomingSignature) + " at position " + typeSigPosition + ", expected 0x" + Long.toHexString(typeSignature));

    if (incomingSignature != this.typeSignature) {
      throw new IllegalStateException("Type signature mismatch: expected " + Long.toHexString(this.typeSignature) + " but got " + Long.toHexString(incomingSignature) + " at position: " + typeSigPosition);
    }

    LOGGER.finer(() -> "RecordPickler deserializing record " + this.userType.getSimpleName() + " buffer remaining bytes: " + buffer.remaining() + " limit: " + buffer.limit() + " capacity: " + buffer.capacity());
    return readFromWire(buffer);
  }

  void serializeRecordComponents(ByteBuffer buffer, T record) {
    IntStream.range(0, componentWriters.length).forEach(i -> {
      final int componentIndex = i; // final for lambda capture
      LOGGER.fine(() -> "RecordPickler " + userType.getSimpleName() + " writing component " + componentIndex + " at position " + buffer.position() + " buffer remaining bytes: " + buffer.remaining() + " limit: " + buffer.limit() + " capacity: " + buffer.capacity());
      componentWriters[componentIndex].accept(buffer, record);
    });
  }

  T readFromWire(ByteBuffer buffer) {
    Object[] components = new Object[componentReaders.length];
    IntStream.range(0, componentReaders.length).forEach(i -> {
      final int componentIndex = i; // final for lambda capture
      final int beforePosition = buffer.position();
      LOGGER.fine(() -> "RecordPickler reading component " + componentIndex + " at position " + beforePosition + " buffer remaining bytes: " + buffer.remaining() + " limit: " + buffer.limit() + " capacity: " + buffer.capacity());
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
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  @Override
  public int maxSizeOf(T object) {
    Objects.requireNonNull(object);
    if (this.userType.isAssignableFrom(object.getClass())) {
      int size = CLASS_SIG_BYTES + Integer.BYTES; // signature bytes then ordinal marker
      size += maxSizeOfRecordComponents(object);
      return size;
    } else if (object instanceof Enum<?>) {
      // For enums, we store the ordinal and type signature
      return Long.BYTES + Integer.BYTES; // typeSignature + ordinal
    } else if (object instanceof Record) {
      final var otherPickler = resolvePicker(object.getClass(), this.enumToTypeSignatureMap);
      LOGGER.fine(() -> "RecordPickler " + userType.getSimpleName() + " maxSizeOf() delegating to other pickler for record type: " + object.getClass().getSimpleName());
      return otherPickler.maxSizeOf(object);
    } else {
      throw new IllegalArgumentException("RecordPickler " + userType.getSimpleName() + " expected a record type " + this.userType.getName() + " but got: " + object.getClass().getName());
    }
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
        LOGGER.fine(() -> "Extracted value is not null, delegating to sizer");
        return delegate.applyAsInt(value);
      }
    };
  }

  static BiConsumer<ByteBuffer, Object> extractAndDelegate(BiConsumer<ByteBuffer, Object> delegate, MethodHandle
      accessor) {
    return (buffer, record) -> {
      final Object value;
      try {
        value = accessor.invokeWithArguments(record);
      } catch (Throwable e) {
        throw new RuntimeException(e.getMessage(), e);
      }
      if (value == null) {
        LOGGER.fine(() -> "Extracted value is null, writing 0L marker");
        buffer.putLong(0L); // write a marker for null
      } else {
        LOGGER.fine(() -> "Extracted value is not null, delegating to writer: " + value);
        delegate.accept(buffer, value);
      }
    };
  }

  ToIntFunction<Object> buildSizerChainFromAST(TypeExpr typeExpr, MethodHandle accessor) {
    LOGGER.fine(() -> "Building sizer chain from AST for type: " + typeExpr.toTreeString());
    return extractAndDelegate(buildSizerChainFromASTInner(typeExpr), accessor);
  }

  ToIntFunction<Object> buildSizerChainFromASTInner(TypeExpr typeExpr) {
    switch (typeExpr) {
      case TypeExpr.PrimitiveValueNode(TypeExpr.PrimitiveValueType primitiveType, Type ignored) -> {
        LOGGER.fine(() -> "Building sizer chain for primitive type: " + primitiveType);
        return Companion.buildPrimitiveValueSizer(primitiveType);
      }
      case TypeExpr.ArrayNode(TypeExpr element) -> {
        switch (element) {
          case TypeExpr.PrimitiveValueNode(TypeExpr.PrimitiveValueType primitiveType, Type ignored) -> {
            LOGGER.fine(() -> "Building sizer chain for primitive array type: " + primitiveType);
            return Companion.buildPrimitiveArraySizerInner(primitiveType);
          }
          case TypeExpr.RefValueNode(TypeExpr.RefValueType refValueType, Type javaType) -> {
            LOGGER.fine(() -> "Building sizer chain for reference array type: " + refValueType);
            final var refValueSizer = buildValueSizerInner(refValueType, javaType);
            return createArraySizerInner(refValueSizer);
          }
          default -> {
            LOGGER.fine(() -> "Building sizer chain for array of container type: " + element.toTreeString());
            final var delegate = buildSizerChainFromASTInner(element);
            return createArraySizerInner(delegate);
          }
        }
      }
      case TypeExpr.RefValueNode(TypeExpr.RefValueType refValueType, Type javaType) -> {
        LOGGER.fine(() -> "Building sizer chain for reference value type: " + refValueType);
        return buildValueSizerInner(refValueType, javaType);
      }
      case TypeExpr.ListNode(TypeExpr element) -> {
        LOGGER.fine(() -> "Building sizer chain for ListNode with element type: " + element.toTreeString());
        final var elementSizer = buildSizerChainFromASTInner(element);
        return createListSizerInner(elementSizer);
      }
      case TypeExpr.MapNode(TypeExpr key, TypeExpr value) -> {
        LOGGER.fine(() -> "Building sizer chain for MapNode with key type: " + key.toTreeString() + " and value type: " + value.toTreeString());
        final var keySizer = buildSizerChainFromASTInner(key);
        final var valueSizer = buildSizerChainFromASTInner(value);
        return createMapSizerInner(keySizer, valueSizer);
      }
      case TypeExpr.OptionalNode(TypeExpr value) -> {
        LOGGER.fine(() -> "Building sizer chain for OptionalNode with value type: " + value.toTreeString());
        final var valueSizer = buildSizerChainFromASTInner(value);
        return createOptionalSizerInner(valueSizer);
      }
    }
  }

  @NotNull ToIntFunction<Object> buildValueSizerInner(TypeExpr.RefValueType refValueType, Type javaType) {
    LOGGER.fine(() -> "Building value sizer inner for RefValueType: " + refValueType + " and Java type: " + javaType);
    if (javaType instanceof Class<?> cls) {
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
        case RECORD -> {
          if (userType.isAssignableFrom(cls)) {
            //noinspection unchecked
            yield (Object obj) -> 1 + Integer.BYTES + CLASS_SIG_BYTES + maxSizeOfRecordComponents((T) obj);
          } else {
            // Delegate to the recordValueSizer for the specific type
            final var otherPickler = resolvePicker(cls, this.enumToTypeSignatureMap);
            LOGGER.fine(() -> "RecordPickler " + userType.getSimpleName() + " buildValueSizerInner() delegating to other pickler for record type: " + cls.getSimpleName());
            yield otherPickler::maxSizeOf;
          }
        }
        case INTERFACE -> (Object obj) -> {
          if (obj instanceof Enum<?> ignored) {
            // For enums, we store the ordinal and type signature
            return Long.BYTES + Integer.BYTES;
          } else if (userType.isAssignableFrom(obj.getClass())) {
            //noinspection unchecked
            return 1 + Integer.BYTES + CLASS_SIG_BYTES + maxSizeOfRecordComponents((T) obj);
          } else if (picklers.containsKey(obj.getClass())) {
            // Delegate to the recordValueSizer for the specific type
            final var otherPickler = resolvePicker(obj.getClass(), this.enumToTypeSignatureMap);
            LOGGER.fine(() -> "RecordPickler " + userType.getSimpleName() + " buildValueSizerInner() delegating to other pickler for interface type: " + obj.getClass().getSimpleName());
            return otherPickler.maxSizeOf(obj);
          } else {
            throw new IllegalArgumentException("Unsupported interface type: " + obj.getClass().getSimpleName());
          }
        };
      };
    } else {
      throw new AssertionError("Unsupported Java type for RefValueType: " + refValueType + ", expected a Class<?> but got: " + javaType.getTypeName());
    }
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
      int size = Integer.BYTES; // size of the marker
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        size += keySizer.applyAsInt(entry.getKey());
        size += valueSizer.applyAsInt(entry.getValue());
      }
      return size;
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
      int length = Array.getLength(inner);
      return Integer.BYTES + IntStream.range(0, length)
          .mapToObj(i -> elementSizer.applyAsInt(Array.get(inner, i)))
          .reduce(0, Integer::sum);
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
      int size = Integer.BYTES; // size of the marker
      for (Object element : list) {
        size += elementSizer.applyAsInt(element);
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
      default -> throw new AssertionError("Unsupported element type for array writer: " + element.toTreeString());
    };
    return (buffer, value) -> {
      Object[] array = (Object[]) value;
      // Write ARRAY_OBJ marker and length
      ZigZagEncoding.putInt(buffer, marker);
      ZigZagEncoding.putInt(buffer, array.length);
      // Write each element
      for (Object item : array) {
        elementWriter.accept(buffer, item);
      }
    };
  }

  static BiConsumer<ByteBuffer, Object> createMapWriterInner(
      BiConsumer<ByteBuffer, Object> keyWriter,
      BiConsumer<ByteBuffer, Object> valueWriter) {
    return (buffer, obj) -> {
      Map<?, ?> map = (Map<?, ?>) obj;
      // Write MAP marker and size
      ZigZagEncoding.putInt(buffer, Constants.MAP.marker());
      ZigZagEncoding.putInt(buffer, map.size());
      // Write each key-value pair
      map.forEach((key, value) -> {
        keyWriter.accept(buffer, key);
        valueWriter.accept(buffer, value);
      });
    };
  }

  static Function<ByteBuffer, Object> createListReader(Function<ByteBuffer, Object> elementReader) {
    return buffer -> {
      int marker = ZigZagEncoding.getInt(buffer);
      assert marker == Constants.LIST.marker() : "Expected LIST marker";
      int size = ZigZagEncoding.getInt(buffer);
      List<Object> list = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        list.add(elementReader.apply(buffer));
      }
      return list;
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
      default -> throw new AssertionError("Unsupported element type for array writer: " + element.toTreeString());
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
      case TypeExpr.ArrayNode(var ignored) -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker != expectedMarker) {
          throw new IllegalStateException("Expected marker " + expectedMarker + " but got " + marker);
        }
        int length = ZigZagEncoding.getInt(buffer);
        Object[][] array = (Object[][]) Array.newInstance(componentType, length);
        Arrays.setAll(array, i -> elementReader.apply(buffer));
        return array;
      };
      default -> throw new AssertionError("Unsupported element type for array writer: " + element.toTreeString());
    };
  }

  static Function<ByteBuffer, Object> createMapReader(
      Function<ByteBuffer, Object> keyReader,
      Function<ByteBuffer, Object> valueReader) {
    return buffer -> {
      int marker = ZigZagEncoding.getInt(buffer);
      assert marker == Constants.MAP.marker() : "Expected MAP marker";
      int size = ZigZagEncoding.getInt(buffer);
      Map<Object, Object> map = new HashMap<>(size);
      for (int i = 0; i < size; i++) {
        Object key = keyReader.apply(buffer);
        Object value = valueReader.apply(buffer);
        map.put(key, value);
      }
      return map;
    };
  }

  BiConsumer<ByteBuffer, Object> buildWriterChainFromAST(TypeExpr typeExpr, MethodHandle accessor) {
    return extractAndDelegate(buildWriterChainFromASTInner(typeExpr), accessor);
  }

  @NotNull BiConsumer<ByteBuffer, Object> buildWriterChainFromASTInner(TypeExpr typeExpr) {
    return switch (typeExpr) {
      case TypeExpr.PrimitiveValueNode(var primitiveType, var ignored) ->
          Companion.buildPrimitiveValueWriterInner(primitiveType);

      case TypeExpr.RefValueNode(var refType, var javaType) -> buildValueWriterInner(refType, javaType);

      case TypeExpr.ArrayNode(TypeExpr element) -> {
        LOGGER.fine(() -> "Building writer chain for array of type: " + element.toTreeString());
        if (element instanceof TypeExpr.PrimitiveValueNode) {
          // Primitive arrays
          var primitiveType = ((TypeExpr.PrimitiveValueNode) element).type();
          yield Companion.buildPrimitiveArrayWriterInner(primitiveType);
        } else if (element instanceof TypeExpr.RefValueNode) {
          // Nested container arrays
          var elementWriter = buildWriterChainFromASTInner(element);
          yield createArrayRefWriter(elementWriter, element);
        } else if (element instanceof TypeExpr.ArrayNode) {
          // nested arrays
        }
        throw new AssertionError("Unsupported element type for array writer: " + element.toTreeString());
      }

      case TypeExpr.ListNode(var element) -> {
        var elementWriter = buildWriterChainFromASTInner(element);
        yield createListWriterInner(elementWriter);
      }

      case TypeExpr.OptionalNode(var wrapped) -> {
        var valueWriter = buildWriterChainFromASTInner(wrapped);
        yield createOptionalWriterInner(valueWriter);
      }

      case TypeExpr.MapNode(var key, var value) -> {
        var keyWriter = buildWriterChainFromASTInner(key);
        var valueWriter = buildWriterChainFromASTInner(value);
        yield createMapWriterInner(keyWriter, valueWriter);
      }
    };
  }

  Function<ByteBuffer, Object> buildReaderChainFromAST(TypeExpr typeExpr) {
    return switch (typeExpr) {
      case TypeExpr.PrimitiveValueNode(var primitiveType, var ignored) ->
          Companion.buildPrimitiveValueReader(primitiveType);

      case TypeExpr.RefValueNode(var refType, var ignored1) -> buildValueReader(refType);

      case TypeExpr.ArrayNode(var element) -> {
        switch (element) {
          case TypeExpr.PrimitiveValueNode(TypeExpr.PrimitiveValueType primitiveType, Type ignored) -> {
            LOGGER.fine(() -> "Building sizer chain for primitive array type: " + primitiveType);
            yield Companion.buildPrimitiveArrayReader(primitiveType);
          }
          case TypeExpr.RefValueNode(TypeExpr.RefValueType refValueType, Type ignored) -> {
            LOGGER.fine(() -> "Building sizer chain for reference array type: " + refValueType);
            final var reader = buildValueReader(refValueType);
            var componentType = Companion.extractComponentType(element);
            yield createArrayReader(reader, componentType, element);
          }
          default -> {
            LOGGER.fine(() -> "Building sizer chain for array of container type: " + element.toTreeString());
            final var elementReader = buildReaderChainFromAST(element);
            var componentType = Companion.extractComponentType(element);
            yield createArrayReader(elementReader, componentType, element);
          }
        }
      }

      case TypeExpr.ListNode(var element) -> {
        var elementReader = buildReaderChainFromAST(element);
        yield createListReader(elementReader);
      }

      case TypeExpr.OptionalNode(var wrapped) -> {
        var valueReader = buildReaderChainFromAST(wrapped);
        yield createOptionalReader(valueReader);
      }

      case TypeExpr.MapNode(var key, var value) -> {
        var keyReader = buildReaderChainFromAST(key);
        var valueReader = buildReaderChainFromAST(value);
        yield createMapReader(keyReader, valueReader);
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
        elementWriter.accept(buffer, item);
      }
    };
  }

  static BiConsumer<ByteBuffer, Object> createOptionalWriterInner(BiConsumer<ByteBuffer, Object> valueWriter) {
    LOGGER.fine(() -> "Creating optional writer with valueWriter: " + valueWriter);
    return (buffer, value) -> {
      Optional<?> optional = (Optional<?>) value;
      if (optional.isEmpty()) {
        ZigZagEncoding.putInt(buffer, Constants.OPTIONAL_EMPTY.marker());
      } else {
        ZigZagEncoding.putInt(buffer, Constants.OPTIONAL_OF.marker());
        valueWriter.accept(buffer, optional.get());
      }
    };
  }

  public static @NotNull BiConsumer<ByteBuffer, Object> buildPrimitiveArrayWriter(TypeExpr.PrimitiveValueType
                                                                                      primitiveType, MethodHandle accessor) {
    LOGGER.fine(() -> "Building writer chain for primitive array type: " + primitiveType);
    return extractAndDelegate(Companion.buildPrimitiveArrayWriterInner(primitiveType), accessor);
  }

  static @NotNull BiConsumer<ByteBuffer, Object> buildPrimitiveValueWriter(TypeExpr.PrimitiveValueType
                                                                               primitiveType, MethodHandle methodHandle) {
    return extractAndDelegate(Companion.buildPrimitiveValueWriterInner(primitiveType), methodHandle);
  }


  @NotNull BiConsumer<ByteBuffer, Object> buildValueWriterInner(final TypeExpr.RefValueType refValueType, Type javaType) {

    LOGGER.fine(() -> "Building writer chain for RefValueType: " + refValueType + " with Java type: " + javaType);
    if (javaType instanceof Class<?> clz) {
      return switch (refValueType) {
        case BOOLEAN -> {
          LOGGER.fine(() -> "Building writer chain for boolean.class primitive type");
          yield (ByteBuffer buffer, Object result) -> buffer.put((byte) ((boolean) result ? 1 : 0));
        }
        case BYTE -> {
          LOGGER.fine(() -> "Building writer chain for byte.class primitive type");
          yield (ByteBuffer buffer, Object result) -> buffer.put((byte) result);
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
        case ENUM -> {
          LOGGER.fine(() -> "Building writer chain for Enum");
          yield (ByteBuffer buffer, Object record) -> {
            if (record instanceof Enum<?> enumValue) {
              // For enums, we store the ordinal and type signature
              final var typeSignature = enumToTypeSignatureMap.get(enumValue.getClass());
              LOGGER.fine(() -> "Writing enum value: " + enumValue + " with type signature: 0x" + Long.toHexString(typeSignature) + " and ordinal: " + enumValue.ordinal() + " at position: " + buffer.position());
              buffer.putLong(typeSignature);
              ZigZagEncoding.putInt(buffer, enumValue.ordinal());
            } else {
              throw new IllegalArgumentException("Expected an Enum but got: " + record.getClass().getName());
            }
          };
        }
        case RECORD -> writeRecordToWire(clz);
        case INTERFACE -> {
          LOGGER.fine(() -> "Building writer chain for Interface");
          yield (ByteBuffer buffer, Object obj) -> {
            if (obj instanceof Enum<?> enumValue) {
              assert enumToTypeSignatureMap.containsKey(enumValue.getClass()) : this + "No type signature found for enum class: " + enumValue.getClass().getName() + ", available signatures: " + enumToTypeSignatureMap.keySet();
              // For enums, we store the ordinal and type signature
              final var typeSignature = enumToTypeSignatureMap.get(enumValue.getClass());
              LOGGER.fine(() -> this + " writing enum value: " + enumValue + " with type signature: 0x" + Long.toHexString(typeSignature) + " and ordinal: " + enumValue.ordinal() + " at position: " + buffer.position());
              buffer.putLong(typeSignature);
              ZigZagEncoding.putInt(buffer, enumValue.ordinal());
            } else if (userType.isAssignableFrom(obj.getClass())) {
              //noinspection unchecked
              writeToWire(buffer, (T) obj);
            } else if (picklers.containsKey(obj.getClass())) {
              // If we have a pickler for this interface type, use it
              final var pickler = picklers.get(obj.getClass());
              LOGGER.fine(() -> "Using pickler for interface type: " + obj.getClass().getSimpleName());
              switch (pickler) {
                case EmptyRecordPickler<?> erp -> buffer.putLong(erp.typeSignature);
                case RecordPickler<?> rp -> writeToWireWitness(rp, buffer, obj);
                default -> throw new AssertionError("Unexpected pickler type: " + pickler.getClass());
              }
            } else {
              throw new IllegalArgumentException("Unsupported interface type: " + obj.getClass().getSimpleName());
            }
          };
        }
      };
    } else {
      throw new AssertionError("Expected a Class type for Java type but got: " + javaType.getClass().getName());
    }
  }

  private @NotNull BiConsumer<ByteBuffer, Object> writeRecordToWire(Class<?> clz) {
    if (userType.isAssignableFrom(clz)) {
      LOGGER.fine(() -> "Building self writer chain for userType " + userType.getSimpleName());
      return (ByteBuffer buffer, Object record) -> {
        //noinspection unchecked
        writeToWire(buffer, (T) record);
      };
    } else {
      LOGGER.fine(() -> "Building delegating writer chain for Record " + clz.getSimpleName());
      return (ByteBuffer buffer, Object record) -> {
        final var p = REGISTRY.computeIfAbsent(record.getClass(), userType1 -> new RecordPickler<>(userType1, enumToTypeSignatureMap));
        switch (p) {
          case EmptyRecordPickler<?> erp -> buffer.putLong(erp.typeSignature);
          case RecordPickler<?> rp -> writeToWireWitness(rp, buffer, record);
          default -> throw new AssertionError("Unexpected pickler type: " + p.getClass());
        }
      };
    }
  }

  @Override
  public String toString() {
    return "RecordPickler{" +
        "userType=" + userType +
        ", typeSignature=" + typeSignature +
        '}';
  }
}
