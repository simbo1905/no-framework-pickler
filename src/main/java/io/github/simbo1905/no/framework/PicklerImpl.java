// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//

package io.github.simbo1905.no.framework;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.simbo1905.no.framework.Constants.INTEGER_VAR;
import static io.github.simbo1905.no.framework.Tag.*;
import static java.nio.charset.StandardCharsets.UTF_8;

/// Unified pickler implementation that handles all reachable types using array-based architecture.
/// Eliminates the need for separate RecordPickler and SealedPickler classes.
final class PicklerImpl<T> implements Pickler<T> {

  record TypeInfo(
      int typeOrdinal,
      long typeSignature,
      Object[] enumConstants,
      Supplier<BiConsumer<ByteBuffer, Object>[]> componentWriters
  ) {
  }

  static final int SAMPLE_SIZE = 32;
  public static final String SHA_256 = "SHA-256";

  static final int CLASS_SIG_BYTES = Long.BYTES;

  static final CompatibilityMode COMPATIBILITY_MODE =
      CompatibilityMode.valueOf(System.getProperty("no.framework.Pickler.Compatibility", "DISABLED"));

  final Class<?> rootClass; // Root class for this pickler, used for discovery and serialization

  // Global lookup tables indexed by ordinal - the core of the unified architecture
  final Class<?>[] userTypes;     // Lexicographically sorted user types which are subclasses of Record or Enum
  Map<Class<?>, TypeInfo> classToTypeInfo;  // Fast user type to ordinal lookup for the write path
  final long[] typeSignatures;    // CLASS_SIG_BYTES SHA256 signatures for backwards compatibility checking

  // Pre-built component metadata arrays for all discovered record types - the core performance optimization:
  final MethodHandle[] recordConstructors;      // Constructor method handles indexed by ordinal
  final MethodHandle[][] componentAccessors;    // [recordOrdinal][componentIndex] -> accessor method handle
  final TypeStructure[][] componentTypes;       // [recordOrdinal][componentIndex] -> type structure
  final BiConsumer<ByteBuffer, Object>[][] componentWriters;  // [recordOrdinal][componentIndex] -> writer chain of delegating lambda eliminating use of `switch` on write the hot path
  final Function<ByteBuffer, Object>[][] componentReaders;    // [recordOrdinal][componentIndex] -> reader chain of delegating lambda eliminating use of `switch` on read the hot path
  final ToIntFunction<Object>[][] componentSizers; // [recordOrdinal][componentIndex] -> sizer lambda

  /// Create a unified pickler for any root type (record, enum, or sealed interface)
  @SuppressWarnings({"unchecked"})
  PicklerImpl(Class<T> rootClass) {
    Objects.requireNonNull(rootClass, "rootClass cannot be null");

    if (!rootClass.isRecord() && !rootClass.isSealed()) {
      throw new IllegalArgumentException("Root class must be a record or sealed interface: " + rootClass.getName());
    }

    LOGGER.info(() -> "Creating unified pickler for root class: " + rootClass.getName());
    this.rootClass = rootClass;

    // Phase 1: Static analysis to discover all reachable user types using recordClassHierarchy
    Set<Class<?>> allReachableClasses = recordClassHierarchy(rootClass, new HashSet<>())
        .filter(clazz -> clazz.isRecord() || clazz.isEnum() || clazz.isSealed())
        .collect(Collectors.toSet());

    LOGGER.info(() -> "Discovered " + allReachableClasses.size() + " reachable user types: " +
        allReachableClasses.stream().map(Class::getSimpleName).toList());

    // Phase 2: Filter out sealed interfaces (keep only concrete records, enums, and arrays for serialization)
    this.userTypes = allReachableClasses.stream()
        .filter(clazz -> !clazz.isSealed()) // Remove sealed interfaces - they're only for discovery
        .sorted(Comparator.comparing(Class::getName))
        .toArray(Class<?>[]::new);

    LOGGER.info(() -> "Filtered to " + userTypes.length + " concrete types (removed sealed interfaces)");

    LOGGER.fine(() -> "Discovered types with indices: " +
        IntStream.range(0, userTypes.length)
            .mapToObj(i -> "[" + i + "]=" + userTypes[i].getName())
            .collect(Collectors.joining(", ")));

    // Build the ONE HashMap for class->ordinal lookup (O(1) for hot path)
    this.classToTypeInfo = IntStream.range(0, userTypes.length)
        .boxed()
        .collect(Collectors.toMap(i
                -> userTypes[i],
            i -> {
              Class<?> userClass = userTypes[i];
              if (userClass.isEnum()) {
                return new TypeInfo(i, 0L, userClass.getEnumConstants(), null);
              } else {
                return new TypeInfo(i, 0L, null, null);
              }
            }));

    LOGGER.finer(() -> "Built classToTypeInfo map: " + classToTypeInfo.entrySet().stream()
        .map(e -> e.getKey().getSimpleName() + "->" + e.getValue()).toList());

    // Pre-allocate metadata arrays for all discovered record types (array-based for O(1) access)
    int numRecordTypes = userTypes.length;
    this.recordConstructors = new MethodHandle[numRecordTypes];
    this.componentAccessors = new MethodHandle[numRecordTypes][];
    this.componentTypes = new TypeStructure[numRecordTypes][];
    this.componentWriters = (BiConsumer<ByteBuffer, Object>[][]) new BiConsumer[numRecordTypes][];
    this.componentReaders = (Function<ByteBuffer, Object>[][]) new Function[numRecordTypes][];
    this.componentSizers = (ToIntFunction<Object>[][]) new ToIntFunction[numRecordTypes][];
    this.typeSignatures = new long[numRecordTypes];

    IntStream.range(0, numRecordTypes).forEach(ordinal -> {
      Class<?> userClass = userTypes[ordinal];
      if (userClass.isRecord()) {
        try {
          metaprogramming(ordinal, userClass);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      } else if (userClass.isEnum()) {
        // Compute enum signature
        typeSignatures[ordinal] = hashEnumSignature(userClass);
        LOGGER.fine(() -> "Computed enum signature for " + userClass.getSimpleName() +
            ": 0x" + Long.toHexString(typeSignatures[ordinal]));
      }
    });

    // Rebuild the map with actual signatures
    this.classToTypeInfo = IntStream.range(0, userTypes.length)
        .boxed()
        .collect(Collectors.toMap(i -> userTypes[i], i ->
            new TypeInfo(i, typeSignatures[i], null, null)));

    LOGGER.finer(() -> "Final classToTypeInfo with signatures: " + classToTypeInfo.entrySet().stream()
        .map(e -> e.getKey().getSimpleName() + " -> " + e.getValue())
        .collect(Collectors.joining(", ")));

    LOGGER.info(() -> "PicklerImpl construction complete - ready for high-performance serialization");
  }

  /// Compute a CLASS_SIG_BYTES signature from class name and component metadata
  /// TODO i am in two minds about using the getSimpleName() or the full getName() or Class.getCanonicalName() or ...
  static long hashRecordSignature(Class<?> clazz, RecordComponent[] components, TypeStructure[] componentTypes) {
    try {
      MessageDigest digest = MessageDigest.getInstance(SHA_256);

      String input = Stream.concat(
          Stream.of(clazz.getSimpleName()),
          IntStream.range(0, components.length)
              .boxed()
              .flatMap(i -> Stream.concat(
                  componentTypes[i].tagTypes().stream()
                      .map(TagWithType::tag)
                      .map(Tag::name),
                  Stream.of(components[i].getName())
              ))
      ).collect(Collectors.joining("!"));

      byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));

      // Convert first CLASS_SIG_BYTES to long
      //      Byte Index:   0       1       2        3        4        5        6        7
      //      Bits:      [56-63] [48-55] [40-47] [32-39] [24-31] [16-23] [ 8-15] [ 0-7]
      //      Shift:      <<56   <<48   <<40    <<32    <<24    <<16    <<8     <<0
      return IntStream.range(0, CLASS_SIG_BYTES)
          .mapToLong(i -> (hash[i] & 0xFFL) << (56 - i * 8))
          .reduce(0L, (a, b) -> a | b);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(SHA_256 + " not available", e);
    }
  }

  /// Compute a CLASS_SIG_BYTES signature from enum class and constant names
  private static long hashEnumSignature(Class<?> enumClass) {
    try {
      MessageDigest digest = MessageDigest.getInstance(SHA_256);

      Object[] enumConstants = enumClass.getEnumConstants();
      assert enumConstants != null : "Not an enum class: " + enumClass;

      String input = Stream.concat(
          Stream.of(enumClass.getSimpleName()),
          Arrays.stream(enumConstants)
              .map(e -> ((Enum<?>) e).name())
      ).collect(Collectors.joining("!"));

      byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));

      return IntStream.range(0, CLASS_SIG_BYTES)
          .mapToLong(i -> (hash[i] & 0xFFL) << (56 - i * 8))
          .reduce(0L, (a, b) -> a | b);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(SHA_256 + " not available", e);
    }
  }

  @SuppressWarnings({"unchecked"})
  void metaprogramming(int ordinal, Class<?> recordClass) throws Exception {
    LOGGER.fine(() -> "Building metadata for ordinal " + ordinal + ": " + recordClass.getSimpleName());

    // Get component accessors and analyze types
    RecordComponent[] components = recordClass.getRecordComponents();
    int numComponents = components.length;

    Class<?>[] parameterTypes = Arrays.stream(components)
        .map(RecordComponent::getType)
        .toArray(Class<?>[]::new);
    var constructor = recordClass.getDeclaredConstructor(parameterTypes);
    recordConstructors[ordinal] = MethodHandles.lookup().unreflectConstructor(constructor);
    componentAccessors[ordinal] = new MethodHandle[numComponents];
    componentTypes[ordinal] = new TypeStructure[numComponents];
    componentWriters[ordinal] = (BiConsumer<ByteBuffer, Object>[]) new BiConsumer[numComponents];
    componentReaders[ordinal] = (Function<ByteBuffer, Object>[]) new Function[numComponents];
    componentSizers[ordinal] = (ToIntFunction<Object>[]) new ToIntFunction[numComponents];

    IntStream.range(0, numComponents).forEach(i -> {
      RecordComponent component = components[i];

      // Get accessor method handle
      try {
        componentAccessors[ordinal][i] = MethodHandles.lookup().unreflect(component.getAccessor());
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Failed to un reflect accessor for " + component.getName(), e);
      }

      final var typeStructure = TypeStructure.analyze(component.getGenericType());
      assert !typeStructure.tagTypes().isEmpty() : "Type structure for " + component.getName() + " should not be empty";

      // Static analysis of component type structure of the component to understand if it has nested containers
      // like array, list, map, optional,  etc
      componentTypes[ordinal][i] = typeStructure;

      if (typeStructure.tagTypes().size() == 1 && typeStructure.tagTypes().getFirst().type().isPrimitive()) {
        final var leafType = typeStructure.tagTypes().getFirst();
        final var primateWriter = createLeafWriter(leafType);
        final var accessor = componentAccessors[ordinal][i];
        componentWriters[ordinal][i] = (buffer, record) -> {
          Object componentValue;
          try {
            componentValue = accessor.invokeWithArguments(record);
          } catch (Throwable e) {
            throw new RuntimeException("Failed to extract component from record:" + record.getClass(), e);
          }
          primateWriter.accept(buffer, componentValue);
        };
        final var primitiveReader = createLeafReader(leafType);
        componentReaders[ordinal][i] = buffer -> {
          try {
            return primitiveReader.apply(buffer);
          } catch (Throwable e) {
            throw new RuntimeException("Failed to read component", e);
          }
        };
        //componentSizers[ordinal][i] = createLeafSizer(leafType);
        final var primitiveSizer = createLeafSizer(ordinal, leafType);
        componentSizers[ordinal][i] = record -> {
          Object componentValue;
          try {
            componentValue = accessor.invokeWithArguments(record);
          } catch (Throwable e) {
            throw new RuntimeException("Failed to extract component from record:" + record.getClass(), e);
          }
          return primitiveSizer.applyAsInt(componentValue);
        };
      } else {
        // Build writer, reader, and sizer chains
        componentWriters[ordinal][i] = buildWriterChain(typeStructure, componentAccessors[ordinal][i]);
        componentReaders[ordinal][i] = buildReaderChain(typeStructure);
        componentSizers[ordinal][i] = buildSizerChain(typeStructure, componentAccessors[ordinal][i]);
      }

    });

    LOGGER.fine(() -> "Completed metadata for " + recordClass.getSimpleName() + " with " + numComponents + " components");

    // Compute and store the type signature for this record
    typeSignatures[ordinal] = hashRecordSignature(recordClass, components, componentTypes[ordinal]);
  }

  /// Serialize record components using pre-built writers array (NO HashMap lookups)
  void serializeRecordComponents(ByteBuffer buffer, Record record, TypeInfo typeInfo) {
    BiConsumer<ByteBuffer, Object>[] writers = componentWriters[typeInfo.typeOrdinal()];
    if (writers == null) {
      throw new IllegalStateException("No writers for ordinal: " + typeInfo.typeOrdinal());
    }

    LOGGER.finer(() -> "Serializing " + writers.length + " components for " + record.getClass().getSimpleName() +
        " at position " + buffer.position());

    // Use pre-built writers - direct array access, no HashMap lookups
    IntStream.range(0, writers.length).forEach(i -> {
      final int componentIndex = i; // final for lambda capture
      LOGGER.finer(() -> "Writing component " + componentIndex + " at position " + buffer.position());
      writers[i].accept(buffer, record);
      LOGGER.finer(() -> "Finished component " + componentIndex + " at position " + buffer.position());
    });
  }

  /// Deserialize record using pre-built readers and constructor
  @SuppressWarnings("unchecked")
  T deserializeRecord(ByteBuffer buffer, int ordinal) {
    Function<ByteBuffer, Object>[] readers = componentReaders[ordinal];
    MethodHandle constructor = recordConstructors[ordinal];

    if (readers == null || constructor == null) {
      throw new IllegalStateException("No readers/constructor for ordinal: " + ordinal);
    }

    LOGGER.finer(() -> "Deserializing " + readers.length + " components at position " + buffer.position());

    // Read components using pre-built readers
    Object[] components = new Object[readers.length];
    for (int i = 0; i < readers.length; i++) {
      final int componentIndex = i; // final for lambda capture
      LOGGER.finer(() -> "Reading component " + componentIndex + " at position " + buffer.position());
      components[i] = readers[i].apply(buffer);
      final Object componentValue = components[i]; // final for lambda capture
      LOGGER.finer(() -> "Read component " + componentIndex + ": " + componentValue + " at position " + buffer.position());
    }

    // Invoke constructor
    try {
      LOGGER.finer(() -> "Constructing record with components: " + Arrays.toString(components));
      return (T) constructor.invokeWithArguments(components);
    } catch (Throwable e) {
      throw new RuntimeException("Failed to construct record", e);
    }
  }

  /// Deserialize record with defaults for missing fields (backwards compatibility)
  @SuppressWarnings("unchecked")
  T deserializeRecordWithDefaults(ByteBuffer buffer, int ordinal) {
    Function<ByteBuffer, Object>[] readers = componentReaders[ordinal];
    MethodHandle constructor = recordConstructors[ordinal];
    TypeStructure[] types = componentTypes[ordinal];

    if (readers == null || constructor == null) {
      throw new IllegalStateException("No readers/constructor for ordinal: " + ordinal);
    }

    LOGGER.info(() -> "DEFAULTED mode: deserializing with potential missing fields for " + userTypes[ordinal].getSimpleName());
    LOGGER.finer(() -> "Expecting " + readers.length + " components at position " + buffer.position());

    // Read components using pre-built readers, use defaults for missing ones
    Object[] components = new Object[readers.length];

    for (int i = 0; i < readers.length; i++) { // TODO remove counting for-loop
      final int componentIndex = i;

      // Check if we have more data to read
      if (!buffer.hasRemaining()) {
        LOGGER.info(() -> "No more data in buffer at component " + componentIndex + ", using defaults for remaining components");
        break;
      }

      // Mark position before attempting to read
      buffer.mark();

      try {
        LOGGER.finer(() -> "Attempting to read component " + componentIndex + " at position " + buffer.position());
        components[i] = readers[i].apply(buffer);
        final Object componentValue = components[i];
        LOGGER.finer(() -> "Read component " + componentIndex + ": " + componentValue);
      } catch (Exception e) {
        // If we can't read this component, reset and use defaults for the rest
        LOGGER.info(() -> "Failed to read component " + componentIndex + ": " + e.getMessage() + ", using defaults");
        buffer.reset();
        break;
      }
    }

    // Fill in default values for missing components
    for (int i = 0; i < components.length; i++) { // TODO remove counting for-loop
      if (components[i] == null && types[i] != null && !types[i].tagTypes().isEmpty()) {
        Tag tag = types[i].tagTypes().getFirst().tag();
        components[i] = getDefaultValue(tag);
        final int idx = i;
        LOGGER.info(() -> "Using default value for component " + idx + ": " + components[idx]);
      }
    }

    // Invoke canonical constructor with all components (including defaults)
    try {
      LOGGER.finer(() -> "Constructing record with all components (including defaults): " + Arrays.toString(components));
      return (T) constructor.invokeWithArguments(components);
    } catch (Throwable e) {
      throw new RuntimeException("Failed to construct record with defaults", e);
    }
  }

  /// Get default value for a component type
  private Object getDefaultValue(Tag tag) {
    return switch (tag) {
      case BOOLEAN -> false;
      case BYTE -> (byte) 0;
      case SHORT -> (short) 0;
      case CHARACTER -> '\0';
      case INTEGER -> 0;
      case LONG -> 0L;
      case FLOAT -> 0.0f;
      case DOUBLE -> 0.0;
      case STRING, UUID, ENUM, RECORD, INTERFACE, ARRAY, LIST, MAP, OPTIONAL -> null;
    };
  }

  /// Calculate max size using pre-built component sizers
  int maxSizeOfRecordComponents(Record record, int ordinal) {
    ToIntFunction<Object>[] sizers = componentSizers[ordinal];
    assert sizers != null : "No sizers for ordinal: " + ordinal;

    int totalSize = 0;
    for (ToIntFunction<Object> sizer : sizers) {
      totalSize += sizer.applyAsInt(record);
    }

    return totalSize;
  }

  /// Build writer chain for a component - creates type-specific writers at construction time
  ///
  /// This method implements the recursive metaprogramming pattern that enables arbitrary nesting
  /// of container types. The pattern follows: (ARRAY|LIST|MAP)* -> (ENUM|RECORD|DOUBLE|INTEGER|...)
  ///
  /// Given a TypeStructure representing a chain of container types ending in a leaf type,
  /// this method builds a delegation chain of writers from right-to-left (leaf to outer container).
  /// Each container writer knows how to serialize its structure (e.g., array length) and then
  /// delegates to its component type's writer for each element.
  ///
  /// Example: For List<List<Double[]>>, the chain would be:
  /// LIST writer -> LIST writer -> ARRAY writer -> DOUBLE writer
  ///
  /// This enables serialization of arbitrarily nested structures like List<Map<String, Integer[]>[]>
  /// without any special-case code - the metaprogramming handles all combinations automatically.
  BiConsumer<ByteBuffer, Object> buildWriterChain(TypeStructure typeStructure, MethodHandle accessor) {
    LOGGER.finer(() -> "Building writer chain for type structure: " +
        typeStructure.tagTypes().stream().map(t -> t.tag().name()).collect(Collectors.joining("->")));

    // Build the type-specific writer chain
    BiConsumer<ByteBuffer, Object> typeWriter = buildTypeWriterChain(typeStructure);

    // Extract component value with null guard then delegate to type-specific writer
    return (buffer, record) -> {
      try {
        Object componentValue = accessor.invokeWithArguments(record);
        LOGGER.finer(() -> "Extracted component value: " + componentValue + " of type: " +
            (componentValue != null ? componentValue.getClass().getSimpleName() : "null"));
        if (componentValue == null) {
          ZigZagEncoding.putInt(buffer, Constants.NULL.ordinal());
          LOGGER.finer(() -> "Writing NULL component at position " + buffer.position());
        } else {
          LOGGER.finer(() -> "Delegating to type writer for value: " + componentValue);
          typeWriter.accept(buffer, componentValue);
        }
      } catch (Throwable e) {
        throw new RuntimeException("Failed to write component", e);
      }
    };
  }

  /// Build type-specific writer chain based on TypeStructure
  BiConsumer<ByteBuffer, Object> buildTypeWriterChain(TypeStructure structure) {

    List<TagWithType> tags = structure.tagTypes();

    LOGGER.finer(() -> "Building type writer chain for structure: " +
        tags.stream().map(tagWithType -> tagWithType.tag() != null ? tagWithType.tag().name() : "null").collect(Collectors.joining(",")));

    // Reverse the tags to process from right to left
    Iterator<TagWithType> reversedTags = tags.reversed().iterator();
    // Maps require a double look back to get the key and value writers
    List<BiConsumer<ByteBuffer, Object>> writers = new ArrayList<>(tags.size());

    // Start with the leaf (rightmost) writer which cannot be a container type
    TagWithType leafTag = reversedTags.next();
    LOGGER.finer(() -> "Creating leaf writer for rightmost tag: " + leafTag.tag() +
        " with type: " + leafTag.type().getSimpleName());
    BiConsumer<ByteBuffer, Object> writer = createLeafWriter(leafTag);
    writers.add(writer);

    TagWithType priorTag = leafTag;
    // For nested collection or option types we Build chain from right to left (reverse order)
    while (reversedTags.hasNext()) {
      final BiConsumer<ByteBuffer, Object> lastWriter = writer; // final required for lambda capture
      TagWithType nextTag = reversedTags.next();
      TagWithType finalPriorTag = priorTag;
      LOGGER.finer(() -> "Processing outer tag: " + nextTag.tag() + " with type: " + nextTag.type().getSimpleName() +
          ", writers.size=" + writers.size() + ", priorTag=" + finalPriorTag.tag());
      writer = switch (nextTag.tag()) {
        case LIST -> createListWriter(lastWriter);
        case OPTIONAL -> createOptionalWriter(lastWriter);
        case MAP -> {
          // MAP structure: [MAP, key_tags..., value_tags...]
          // Keys must be value types (single tag), but values can be complex
          // When building in reverse order, we build value chain first, then key
          // The key writer is in lastWriter, value writer root is at writers.size() - 2
          LOGGER.finer(() -> "MAP case: keyWriter=lastWriter, valueWriter at index " + (writers.size() - 2));
          // Value writer is not the last one (that's the key), but the one before it
          final var valueWriter = writers.get(writers.size() - 2);
          yield createMapWriter(lastWriter, valueWriter);
        }
        case ARRAY -> {
          // Use MetaTag for cleaner switching
          BiConsumer<ByteBuffer, Object> arrayWriter = switch (priorTag.tag().metaTag()) {
            case VALUE -> createValueTypeWriter(priorTag);
            case CONTAINER, MIXED -> (buffer, value) -> {
              LOGGER.finer(() -> "Delegating ARRAY for tag " + finalPriorTag.tag() + " with length=" + Array.getLength(value) + " at position " + buffer.position());
              ZigZagEncoding.putInt(buffer, Constants.ARRAY.marker());
              int length = Array.getLength(value);
              ZigZagEncoding.putInt(buffer, length);
              IntStream.range(0, length).forEach(i -> {
                Object element = Array.get(value, i);
                lastWriter.accept(buffer, element);
              });
            };
          };
          yield arrayWriter;
        }
        default -> createLeafWriter(nextTag);
      };
      writers.add(writer);
      priorTag = nextTag; // Update prior tag for next iteration
    }

    LOGGER.finer(() -> "Final writer chain has " + writers.size() + " writers");
    return writer;
  }

  // TODO we might see built-in keys and values here in which case we can make it a MAP_VALUES and array encord the keys and values for performance
  BiConsumer<ByteBuffer, Object> createMapWriter(BiConsumer<ByteBuffer, Object> keyWriter, BiConsumer<ByteBuffer, Object> valueWriter) {
    return (buffer, obj) -> {
      Map<?, ?> map = (Map<?, ?>) obj;

      // Write MAP marker
      ZigZagEncoding.putInt(buffer, Constants.MAP.marker());

      // Write size
      int size = map.size();
      ZigZagEncoding.putInt(buffer, size);

      LOGGER.fine(() -> "Writing Map with " + size + " entries");

      // Write each key-value pair
      map.forEach((key, value) -> {
        LOGGER.finer(() -> "Writing key: " + key + " of type " + key.getClass().getSimpleName());
        keyWriter.accept(buffer, key);
        LOGGER.finer(() -> "Writing value: " + value + " of type " + value.getClass().getSimpleName());
        valueWriter.accept(buffer, value);
      });
    };
  }

  /// Create enum writer with runtime ordinal lookup
  BiConsumer<ByteBuffer, Object> createEnumWriter(Class<?> enumClass) {
    return (buffer, value) -> {
      Enum<?> enumValue = (Enum<?>) value;
      final TypeInfo typeInfo = classToTypeInfo.get(enumValue.getClass());
      final int ordinal = typeInfo.typeOrdinal();
      final long typeSignature = typeInfo.typeSignature();
      LOGGER.finer(() -> "Writing ENUM ordinal=" + ordinal + " wire=" + (ordinal + 1) + " for " + enumValue.getClass().getName() + " constant ordinal=" + enumValue.ordinal());
      ZigZagEncoding.putInt(buffer, Constants.ENUM.marker());
      ZigZagEncoding.putInt(buffer, ordinal + 1);
      // Write signature for enum class
      buffer.putLong(typeSignature);
      // Write enum constant ordinal (not name) for better compression
      ZigZagEncoding.putInt(buffer, enumValue.ordinal());
    };
  }

  /// Create record writer that handles both records and sealed interface polymorphism
  BiConsumer<ByteBuffer, Object> createRecordWriter(Class<?> recordClass) {
    return (buffer, value) -> {
      // we cannot hoist this out as we may have a sealed interface that permits both records and enums
      final TypeInfo typeInfo = classToTypeInfo.get(value.getClass());
      assert typeInfo != null : "Unknown record type: " + value.getClass();
      final int ordinal = typeInfo.typeOrdinal();
      final long typeSignature = typeInfo.typeSignature();
      LOGGER.finer(() -> "Writing RECORD ordinal=" + ordinal + " wire=" + (ordinal + 1) + " for " + recordClass.getName());
      ZigZagEncoding.putInt(buffer, Constants.RECORD.marker());
      if (value instanceof Record record) {
        ZigZagEncoding.putInt(buffer, ordinal + 1);
        LOGGER.finer(() -> "Writing signature 0x" + Long.toHexString(typeSignature) + " for " + record.getClass().getSimpleName());
        buffer.putLong(typeSignature);
        serializeRecordComponents(buffer, record, typeInfo);
      }
    };
  }

  /// Create interface writer for sealed interfaces that permit both records and enums
  BiConsumer<ByteBuffer, Object> createInterfaceWriter(Class<?> interfaceClass) {
    return (buffer, value) -> {
      if (value instanceof Enum<?> enumValue) {
        // Delegate to enum writer logic
        final TypeInfo typeInfo = classToTypeInfo.get(enumValue.getClass());
        assert typeInfo != null : "Unknown enum type: " + enumValue.getClass();
        final int ordinal = typeInfo.typeOrdinal();
        final long typeSignature = typeInfo.typeSignature();
        LOGGER.finer(() -> "Writing ENUM ordinal=" + ordinal + " wire=" + (ordinal + 1) + " for " + enumValue.getClass().getName());
        ZigZagEncoding.putInt(buffer, Constants.ENUM.marker());
        ZigZagEncoding.putInt(buffer, ordinal + 1);
        buffer.putLong(typeSignature);
        ZigZagEncoding.putInt(buffer, enumValue.ordinal());
      } else if (value instanceof Record record) {
        // Delegate to record writer logic
        final TypeInfo typeInfo = classToTypeInfo.get(record.getClass());
        assert typeInfo != null : "Unknown record type: " + record.getClass();
        final int ordinal = typeInfo.typeOrdinal();
        final long typeSignature = typeInfo.typeSignature();
        LOGGER.finer(() -> "Writing RECORD ordinal=" + ordinal + " wire=" + (ordinal + 1) + " for " + record.getClass().getName());
        ZigZagEncoding.putInt(buffer, Constants.RECORD.marker());
        ZigZagEncoding.putInt(buffer, ordinal + 1);
        LOGGER.finer(() -> "Writing signature 0x" + Long.toHexString(typeSignature) + " for " + record.getClass().getSimpleName());
        buffer.putLong(typeSignature);
        serializeRecordComponents(buffer, record, typeInfo);
      } else {
        throw new IllegalArgumentException("Unexpected type for sealed interface: " + value.getClass());
      }
    };
  }

  /// Create leaf writer for primitive/basic types - NO runtime type checking
  BiConsumer<ByteBuffer, Object> createLeafWriter(TagWithType leafType) {
    LOGGER.fine(() -> "Creating leaf writer for tag: " + leafType.tag() +
        " with type: " + leafType.type().getSimpleName());
    return switch (leafType.tag()) {
      case BOOLEAN -> (buffer, value) -> {
        LOGGER.fine(() -> "Writing BOOLEAN: " + value);
        ZigZagEncoding.putInt(buffer, Constants.BOOLEAN.marker());
        buffer.put(((Boolean) value) ? (byte) 1 : (byte) 0);
      };
      case BYTE -> (buffer, value) -> {
        LOGGER.fine(() -> "Writing BYTE: " + value);
        ZigZagEncoding.putInt(buffer, Constants.BYTE.marker());
        buffer.put((Byte) value);
      };
      case SHORT -> (buffer, value) -> {
        LOGGER.fine(() -> "Writing SHORT: " + value);
        ZigZagEncoding.putInt(buffer, Constants.SHORT.marker());
        buffer.putShort((Short) value);
      };
      case CHARACTER -> (buffer, value) -> {
        LOGGER.fine(() -> "Writing CHARACTER: " + value);
        ZigZagEncoding.putInt(buffer, Constants.CHARACTER.marker());
        buffer.putChar((Character) value);
      };
      case INTEGER -> (buffer, value) -> {
        final Integer intValue = (Integer) value;
        LOGGER.fine(() -> "Writing INTEGER: " + intValue);
        if (ZigZagEncoding.sizeOf(intValue) < Integer.BYTES) {
          ZigZagEncoding.putInt(buffer, Constants.INTEGER_VAR.marker());
          ZigZagEncoding.putInt(buffer, intValue);
        } else {
          ZigZagEncoding.putInt(buffer, Constants.INTEGER.marker());
          buffer.putInt(intValue);
        }
      };
      case LONG -> (buffer, value) -> {
        final Long longValue = (Long) value;
        LOGGER.fine(() -> "Writing LONG: " + longValue);
        if (ZigZagEncoding.sizeOf(longValue) < Long.BYTES) {
          ZigZagEncoding.putInt(buffer, Constants.LONG_VAR.marker());
          ZigZagEncoding.putLong(buffer, longValue);
        } else {
          ZigZagEncoding.putInt(buffer, Constants.LONG.marker());
          buffer.putLong(longValue);
        }
      };
      case FLOAT -> (buffer, value) -> {
        LOGGER.fine(() -> "Writing FLOAT: " + value);
        ZigZagEncoding.putInt(buffer, Constants.FLOAT.marker());
        buffer.putFloat((Float) value);
      };
      case DOUBLE -> (buffer, value) -> {
        LOGGER.fine(() -> "Writing DOUBLE: " + value);
        ZigZagEncoding.putInt(buffer, Constants.DOUBLE.marker());
        buffer.putDouble((Double) value);
      };
      case STRING -> (buffer, value) -> {
        LOGGER.fine(() -> "Writing STRING: " + value);
        ZigZagEncoding.putInt(buffer, Constants.STRING.marker());
        byte[] bytes = ((String) value).getBytes(UTF_8);
        ZigZagEncoding.putInt(buffer, bytes.length);
        buffer.put(bytes);
      };
      case UUID -> (buffer, value) -> {
        LOGGER.fine(() -> "Writing UUID: " + value);
        ZigZagEncoding.putInt(buffer, Constants.UUID.marker());
        UUID uuid = (UUID) value;
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
      };
      case ENUM -> createEnumWriter(leafType.type());
      case RECORD -> createRecordWriter(leafType.type());
      case INTERFACE -> createInterfaceWriter(leafType.type());
      default -> throw new IllegalArgumentException("No leaf writer for tag: " + leafType);
    };
  }

  /// Create array writer for enum arrays with pre-computed ordinal
  BiConsumer<ByteBuffer, Object> createEnumArrayWriter(Class<?> enumClass) {
    final var arrayMarker = Constants.ARRAY.marker();
    final var typeInfo = classToTypeInfo.get(enumClass);
    final var userOrdinal = typeInfo.typeOrdinal();
    final var typeSignature = typeInfo.typeSignature();
    return (buffer, value) -> {
      LOGGER.finer(() -> "Delegating ARRAY for tag " + ENUM + " with length=" + Array.getLength(value) + " at position " + buffer.position());
      final var enums = (Enum<?>[]) value;
      ZigZagEncoding.putInt(buffer, arrayMarker);
      ZigZagEncoding.putInt(buffer, userOrdinal + 1);  // 1-indexed on wire
      int length = enums.length;
      ZigZagEncoding.putInt(buffer, length);
      for (Enum<?> enumValue : enums) {
        ZigZagEncoding.putInt(buffer, enumValue.ordinal());
      }
    };
  }

  /// Create array writer for primitive arrays using tag-based logic
  BiConsumer<ByteBuffer, Object> createValueTypeWriter(final TagWithType typeWithTag) {
    final var arrayMarker = Constants.ARRAY.marker();
    LOGGER.finer(() -> "Creating leaf ARRAY writer for element tag: " + typeWithTag.tag() +
        " element type: " + typeWithTag.type().getSimpleName() +
        " outerMarker: " + arrayMarker);
    return switch (typeWithTag.tag()) {
      case BYTE -> (buffer, value) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag " + BYTE + " with length=" + Array.getLength(value) + " at position " + buffer.position());
        final var bytes = (byte[]) value;
        ZigZagEncoding.putInt(buffer, arrayMarker);
        ZigZagEncoding.putInt(buffer, Constants.BYTE.marker());
        int length = bytes.length;
        ZigZagEncoding.putInt(buffer, length);
        buffer.put(bytes);
      };
      case BOOLEAN -> (buffer, value) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag " + BOOLEAN + " with length=" + Array.getLength(value) + " at position " + buffer.position());
        final var booleans = (boolean[]) value;
        ZigZagEncoding.putInt(buffer, arrayMarker);
        ZigZagEncoding.putInt(buffer, Constants.BOOLEAN.marker());
        int length = booleans.length;
        ZigZagEncoding.putInt(buffer, length);
        BitSet bitSet = new BitSet(length);
        // Create a BitSet and flip bits to try where necessary
        IntStream.range(0, length)
            .filter(i -> booleans[i])
            .forEach(bitSet::set);
        byte[] bytes = bitSet.toByteArray();
        ZigZagEncoding.putInt(buffer, bytes.length);
        buffer.put(bytes);
      };
      case INTEGER -> (buffer, value) -> {
        final var integers = (int[]) value;
        final var length = Array.getLength(value);
        final var sampleAverageSize = length > 0 ? estimateAverageSizeInt(integers, length) : 1;
        // Here we must be saving one byte per integer to justify the encoding cost
        if (sampleAverageSize < Integer.BYTES - 1) {
          LOGGER.finer(() -> "Delegating ARRAY for tag " + INTEGER_VAR + " with length=" + Array.getLength(value) + " at position " + buffer.position());
          ZigZagEncoding.putInt(buffer, arrayMarker);
          ZigZagEncoding.putInt(buffer, Constants.INTEGER_VAR.marker());
          ZigZagEncoding.putInt(buffer, length);
          for (int i : integers) {
            ZigZagEncoding.putInt(buffer, i);
          }
        } else {
          LOGGER.finer(() -> "Delegating ARRAY for tag " + INTEGER + " with length=" + Array.getLength(value) + " at position " + buffer.position());
          ZigZagEncoding.putInt(buffer, arrayMarker);
          ZigZagEncoding.putInt(buffer, Constants.INTEGER.marker());
          ZigZagEncoding.putInt(buffer, length);
          for (int i : integers) {
            buffer.putInt(i);
          }
        }
      };
      case LONG -> (buffer, value) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag " + LONG + " with length=" + Array.getLength(value) + " at position " + buffer.position());
        final var longs = (long[]) value;
        final var length = Array.getLength(value);
        final var sampleAverageSize = length > 0 ? estimateAverageSizeLong(longs, length) : 1;
        // Require 1 byte saving if we sampled the whole array. Require 2 byte saving if we did not sample the whole array.
        if ((length <= SAMPLE_SIZE && sampleAverageSize < Long.BYTES - 1) ||
            (length > SAMPLE_SIZE && sampleAverageSize < Long.BYTES - 2)) {
          LOGGER.fine(() -> "Writing LONG_VAR array - position=" + buffer.position() + " length=" + length);
          ZigZagEncoding.putInt(buffer, arrayMarker);
          ZigZagEncoding.putInt(buffer, Constants.LONG_VAR.marker());
          ZigZagEncoding.putInt(buffer, length);
          for (long i : longs) {
            ZigZagEncoding.putLong(buffer, i);
          }
        } else {
          LOGGER.fine(() -> "Writing LONG array - position=" + buffer.position() + " length=" + length);
          ZigZagEncoding.putInt(buffer, arrayMarker);
          ZigZagEncoding.putInt(buffer, Constants.LONG.marker());
          ZigZagEncoding.putInt(buffer, length);
          for (long i : longs) {
            buffer.putLong(i);
          }
        }
      };
      case FLOAT -> (buffer, value) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag " + FLOAT + " with length=" + Array.getLength(value) + " at position " + buffer.position());
        final var floats = (float[]) value;
        final var length = Array.getLength(value);
        ZigZagEncoding.putInt(buffer, arrayMarker);
        ZigZagEncoding.putInt(buffer, Constants.FLOAT.marker());
        ZigZagEncoding.putInt(buffer, length);
        for (float f : floats) {
          buffer.putFloat(f);
        }
      };
      case DOUBLE -> (buffer, value) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag " + DOUBLE + " with length=" + Array.getLength(value) + " at position " + buffer.position());
        final var doubles = (double[]) value;
        final var length = Array.getLength(value);
        ZigZagEncoding.putInt(buffer, arrayMarker);
        ZigZagEncoding.putInt(buffer, Constants.DOUBLE.marker());
        ZigZagEncoding.putInt(buffer, length);
        for (double d : doubles) {
          buffer.putDouble(d);
        }
      };
      case STRING -> (buffer, value) -> {
        if (value == null) {
          LOGGER.finer(() -> "Writing null STRING array at position " + buffer.position());
          ZigZagEncoding.putInt(buffer, Constants.NULL.marker());
          return;
        }
        final var strings = (String[]) value;
        final var length = Array.getLength(value);
        LOGGER.finer(() -> "Delegating ARRAY for tag " + STRING + " with length=" + length + " at position " + buffer.position());
        ZigZagEncoding.putInt(buffer, arrayMarker);
        ZigZagEncoding.putInt(buffer, Constants.STRING.marker());
        ZigZagEncoding.putInt(buffer, length);
        for (String s : strings) {
          if (s == null) {
            ZigZagEncoding.putInt(buffer, Constants.NULL.marker());
          } else {
            byte[] bytes = s.getBytes(UTF_8);
            ZigZagEncoding.putInt(buffer, bytes.length);
            buffer.put(bytes);
          }
        }
      };
      case UUID -> (buffer, value) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag " + UUID + " with length=" + Array.getLength(value) + " at position " + buffer.position());
        final var uuids = (UUID[]) value;
        final var length = Array.getLength(value);
        ZigZagEncoding.putInt(buffer, arrayMarker);
        ZigZagEncoding.putInt(buffer, Constants.UUID.marker());
        ZigZagEncoding.putInt(buffer, length);
        for (UUID uuid : uuids) {
          buffer.putLong(uuid.getMostSignificantBits());
          buffer.putLong(uuid.getLeastSignificantBits());
        }
      };
      case SHORT -> (buffer, value) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag " + SHORT + " with length=" + Array.getLength(value) + " at position " + buffer.position());
        final var shorts = (short[]) value;
        final var length = Array.getLength(value);
        ZigZagEncoding.putInt(buffer, arrayMarker);
        ZigZagEncoding.putInt(buffer, Constants.SHORT.marker());
        ZigZagEncoding.putInt(buffer, length);
        for (short s : shorts) {
          buffer.putShort(s);
        }
      };
      case CHARACTER -> (buffer, value) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag " + CHARACTER + " with length=" + Array.getLength(value) + " at position " + buffer.position());
        final var chars = (char[]) value;
        final var length = Array.getLength(value);
        ZigZagEncoding.putInt(buffer, arrayMarker);
        ZigZagEncoding.putInt(buffer, Constants.CHARACTER.marker());
        ZigZagEncoding.putInt(buffer, length);
        for (char c : chars) {
          buffer.putChar(c);
        }
      };
      case OPTIONAL -> (buffer, value) -> {
        LOGGER.finer(() -> "Delegating ARRAY for tag " + OPTIONAL + " with length=" + Array.getLength(value) + " at position " + buffer.position());
        final var optional = (Optional<?>) value;
        ZigZagEncoding.putInt(buffer, arrayMarker);
        if (optional.isEmpty()) {
          ZigZagEncoding.putInt(buffer, Constants.OPTIONAL_EMPTY.marker());
        } else {
          ZigZagEncoding.putInt(buffer, Constants.OPTIONAL_OF.marker());
          Object innerValue = optional.get();
          createLeafWriter(typeWithTag).accept(buffer, innerValue);
        }
      };
      case ENUM -> createEnumArrayWriter(typeWithTag.type());
      default -> throw new IllegalArgumentException("Unsupported array type for direct writer: " + typeWithTag.tag());
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

  /// Create writer for Optional values
  BiConsumer<ByteBuffer, Object> createOptionalWriter(BiConsumer<ByteBuffer, Object> elementWriter) {
    return (buffer, value) -> {
      Optional<?> optional = (Optional<?>) value;
      if (optional.isEmpty()) {
        LOGGER.fine(() -> "Writing OPTIONAL_EMPTY");
        ZigZagEncoding.putInt(buffer, Constants.OPTIONAL_EMPTY.marker());
      } else {
        LOGGER.fine(() -> "Writing OPTIONAL_OF");
        ZigZagEncoding.putInt(buffer, Constants.OPTIONAL_OF.marker());
        elementWriter.accept(buffer, optional.get());
      }
    };
  }

  /// Create writer for List values
  BiConsumer<ByteBuffer, Object> createListWriter(BiConsumer<ByteBuffer, Object> elementWriter) {
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

  /// Build reader chain for a component
  /// Use `assert` to check that the read back type match what we expected
  ///
  /// This method implements the recursive metaprogramming pattern for deserialization,
  /// mirroring the writer chain pattern. Given a TypeStructure representing nested containers,
  /// it builds a delegation chain of readers from right-to-left (leaf to outer container).
  ///
  /// The pattern follows: (ARRAY|LIST|MAP)* -> (ENUM|RECORD|DOUBLE|INTEGER|...)
  ///
  /// Each container reader knows how to deserialize its structure (e.g., read array length)
  /// and then delegates to its component type's reader for each element. This enables
  /// deserialization of arbitrarily nested structures without special-case code.
  ///
  /// Example: For Map<String, List<Integer>[]>, the chain would be:
  /// MAP reader -> STRING reader (for keys) and ARRAY reader -> LIST reader -> INTEGER reader (for values)
  Function<ByteBuffer, Object> buildReaderChain(TypeStructure typeStructure) {
    // Build the type-specific reader chain
    Function<ByteBuffer, Object> typeReader = buildTypeReaderChain(typeStructure);

    // Add null guard - check for NULL marker first
    return buffer -> {
      buffer.mark();
      int marker = ZigZagEncoding.getInt(buffer);
      if (marker == 0) {
        LOGGER.fine(() -> "Reading NULL component");
        return null;
      } else {
        buffer.reset(); // Reset to before marker
        return typeReader.apply(buffer);
      }
    };
  }

  /// Build type-specific reader chain based on TypeStructure
  Function<ByteBuffer, Object> buildTypeReaderChain(TypeStructure structure) {
    List<TagWithType> tags = structure.tagTypes();

    // For complex types, build chain from right to left
    Iterator<TagWithType> reversedTags = tags.reversed().iterator();
    // Maps require a double look back to get the key and value readers
    List<Function<ByteBuffer, Object>> readers = new ArrayList<>(tags.size());

    TagWithType leafTag = reversedTags.next();
    Function<ByteBuffer, Object> reader = createLeafReader(leafTag);
    readers.add(reader);

    TagWithType priorTag = leafTag;
    while (reversedTags.hasNext()) {
      final Function<ByteBuffer, Object> innerReader = reader;
      TagWithType tag = reversedTags.next();
      final TagWithType finalPriorTag = priorTag; // Make final for lambda capture
      reader = switch (tag.tag()) {
        case LIST -> createListReader(innerReader);
        case OPTIONAL -> createOptionalReader(innerReader);
        case MAP -> {
          // MAP structure: [MAP, key_tags..., value_tags...]
          // Keys must be value types (single tag), but values can be complex
          // When building in reverse order, we build value chain first, then key
          // The key reader is in innerReader, value reader root is at readers.size() - 2
          final var keyReader = innerReader;
          final var valueReader = readers.get(readers.size() - 2);
          yield createMapReader(keyReader, valueReader);
        }
        case ARRAY -> {
          LOGGER.finer(() -> "Building ARRAY reader for priorTag: " + finalPriorTag.tag() + " type: " + finalPriorTag.type().getSimpleName());
          // Match the writer pattern - check if priorTag is a primitive or complex type
          // Use MetaTag for cleaner switching
          yield switch (finalPriorTag.tag().metaTag()) {
            case VALUE -> createLeafArrayReader(finalPriorTag);
            case CONTAINER, MIXED -> {
              LOGGER.finer(() -> "Creating delegating array reader for tag: " + finalPriorTag.tag());
              yield createContainerArrayReader(innerReader, finalPriorTag);
            }
          };
        }
        default -> createLeafReader(tag);
      };
      readers.add(reader);
      priorTag = tag; // Update prior tag for next iteration
    }

    return reader;
  }

  Function<ByteBuffer, Object> createLeafArrayReader(TagWithType priorTag) {
    Function<ByteBuffer, Object> reader = switch (priorTag.tag()) {
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
      case INTEGER -> (buffer) -> {
        // Check if we have a variable-length integer array
        int priorTagValue = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "Read INTEGER array element type marker: " + priorTagValue + " expecting INTEGER_VAR=" + Constants.INTEGER_VAR.marker() + " or INTEGER=" + Constants.INTEGER.marker());
        if (priorTagValue == Constants.INTEGER_VAR.marker()) {
          int length = ZigZagEncoding.getInt(buffer);
          int[] integers = new int[length];
          IntStream.range(0, length).forEach(i -> integers[i] = ZigZagEncoding.getInt(buffer));
          return integers;
        } else if (priorTagValue == Constants.INTEGER.marker()) {
          // Fixed-length integer array
          int length = ZigZagEncoding.getInt(buffer);
          int[] integers = new int[length];
          IntStream.range(0, length).forEach(i -> integers[i] = buffer.getInt());
          return integers;
        } else throw new IllegalStateException("Expected INTEGER or INTEGER_VAR marker but got: " + priorTagValue);
      };
      case LONG -> (buffer) -> {
        // Check if we have a variable-length long array
        int priorTagValue = ZigZagEncoding.getInt(buffer);
        if (priorTagValue == Constants.LONG_VAR.marker()) {
          int length = ZigZagEncoding.getInt(buffer);
          long[] longs = new long[length];
          IntStream.range(0, length).forEach(i -> longs[i] = ZigZagEncoding.getLong(buffer));
          return longs;
        } else if (priorTagValue == Constants.LONG.marker()) {
          // Fixed-length long array
          int length = ZigZagEncoding.getInt(buffer);
          long[] longs = new long[length];
          IntStream.range(0, length).forEach(i -> longs[i] = buffer.getLong());
          return longs;
        } else throw new IllegalStateException("Expected LONG or LONG_VAR marker but got: " + priorTagValue);
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
      case STRING -> (buffer) -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker == Constants.NULL.marker()) {
          LOGGER.finer(() -> "Reading null STRING array");
          return null;
        }
        assert marker == Constants.STRING.marker() : "Expected STRING marker but got: " + marker;
        int length = ZigZagEncoding.getInt(buffer);
        String[] strings = new String[length];
        IntStream.range(0, length).forEach(i -> {
          final int strLengthOrMarker = ZigZagEncoding.getInt(buffer);
          if (strLengthOrMarker == Constants.NULL.marker()) {
            strings[i] = null;
          } else {
            byte[] bytes = new byte[strLengthOrMarker];
            buffer.get(bytes);
            strings[i] = new String(bytes, UTF_8);
          }
        });
        return strings;
      };
      case UUID -> (buffer) -> {
        int marker = ZigZagEncoding.getInt(buffer);
        assert marker == Constants.UUID.marker() : "Expected UUID marker but got: " + marker;
        int length = ZigZagEncoding.getInt(buffer);
        UUID[] uuids = new UUID[length];
        IntStream.range(0, length).forEach(i -> {
          long mostSigBits = buffer.getLong();
          long leastSigBits = buffer.getLong();
          uuids[i] = new UUID(mostSigBits, leastSigBits);
        });
        return uuids;
      };
      case ENUM -> createEnumArrayReader(priorTag);
      default -> throw new IllegalStateException("Unsupported array type marker: " + priorTag);
    };
    return buffer -> {
      int marker = ZigZagEncoding.getInt(buffer);
      LOGGER.finer(() -> "Read ARRAY marker: " + marker + " expected: " + Constants.ARRAY.marker() + " at position " + buffer.position());
      assert (marker == Constants.ARRAY.marker()) : "Expected ARRAY marker but got: " + marker;
      return reader.apply(buffer);
    };
  }

  /// Create array reader for container element types that delegates to inner reader
  Function<ByteBuffer, Object> createContainerArrayReader(Function<ByteBuffer, Object> elementReader, TagWithType finalPriorTag) {
    return buffer -> {
      int arrayMarker = ZigZagEncoding.getInt(buffer);
      LOGGER.finer(() -> "Read ARRAY marker in delegating reader: " + arrayMarker + " expected: " + Constants.ARRAY.marker());
      assert arrayMarker == Constants.ARRAY.marker() : "Expected ARRAY marker but got: " + arrayMarker;

      int length = ZigZagEncoding.getInt(buffer);
      LOGGER.finer(() -> "Reading delegating array with length: " + length);

      final var array = Array.newInstance(finalPriorTag.type(), length);

      IntStream.range(0, length)
          .forEach(i -> Array.set(array, i, elementReader.apply(buffer)));

      return array;
    };
  }

  Function<ByteBuffer, Object> createEnumArrayReader(TagWithType priorTag) {
    final var userType = priorTag.type();
    LOGGER.finer(() -> "Creating enum array reader for type: " + userType + " isEnum: " + userType.isEnum());
    final var values = userType.getEnumConstants();
    assert values != null : "Expected ENUM type but got: " + userType.getSimpleName();
    return (buffer) -> {
      int marker = ZigZagEncoding.getInt(buffer);
      assert userType.equals(userTypes[marker - 1]) : "Expected ENUM type " + userTypes[marker - 1].getSimpleName() + " but got: " + userType.getSimpleName();
      int length = ZigZagEncoding.getInt(buffer);
      Enum<?>[] enums = (Enum<?>[]) Array.newInstance(userType, length);
      IntStream.range(0, length).forEach(i -> {
        int wireOrdinal = ZigZagEncoding.getInt(buffer);
        enums[i] = (Enum<?>) values[wireOrdinal];
      });
      return enums;
    };
  }

  /// Create leaf reader for primitive/basic types
  Function<ByteBuffer, Object> createLeafReader(TagWithType leafTag) {
    return switch (leafTag.tag()) {
      case BOOLEAN -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker != Constants.BOOLEAN.marker()) {
          throw new IllegalStateException("Expected BOOLEAN marker but got: " + marker);
        }
        boolean value = buffer.get() != 0;
        LOGGER.fine(() -> "Read Boolean: " + value);
        return value;
      };
      case BYTE -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker != Constants.BYTE.marker()) {
          throw new IllegalStateException("Expected BYTE marker but got: " + marker);
        }
        byte value = buffer.get();
        LOGGER.fine(() -> "Read Byte: " + value);
        return value;
      };
      case SHORT -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker != Constants.SHORT.marker()) {
          throw new IllegalStateException("Expected SHORT marker but got: " + marker);
        }
        short value = buffer.getShort();
        LOGGER.fine(() -> "Read Short: " + value);
        return value;
      };
      case CHARACTER -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker != Constants.CHARACTER.marker()) {
          throw new IllegalStateException("Expected CHARACTER marker but got: " + marker);
        }
        char value = buffer.getChar();
        LOGGER.fine(() -> "Read Character: " + value);
        return value;
      };
      case INTEGER -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker == Constants.INTEGER_VAR.marker()) {
          int value = ZigZagEncoding.getInt(buffer);
          LOGGER.fine(() -> "Read Integer (ZigZag): " + value);
          return value;
        } else if (marker == Constants.INTEGER.marker()) {
          int value = buffer.getInt();
          LOGGER.fine(() -> "Read Integer: " + value);
          return value;
        } else {
          throw new IllegalStateException("Expected INTEGER marker but got: " + marker);
        }
      };
      case LONG -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker == Constants.LONG_VAR.marker()) {
          long value = ZigZagEncoding.getLong(buffer);
          LOGGER.fine(() -> "Read Long (ZigZag): " + value);
          return value;
        } else if (marker == Constants.LONG.marker()) {
          long value = buffer.getLong();
          LOGGER.fine(() -> "Read Long: " + value);
          return value;
        } else {
          throw new IllegalStateException("Expected LONG marker but got: " + marker);
        }
      };
      case FLOAT -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker != Constants.FLOAT.marker()) {
          throw new IllegalStateException("Expected FLOAT marker but got: " + marker);
        }
        float value = buffer.getFloat();
        LOGGER.fine(() -> "Read Float: " + value);
        return value;
      };
      case DOUBLE -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker != Constants.DOUBLE.marker()) {
          throw new IllegalStateException("Expected DOUBLE marker but got: " + marker);
        }
        double value = buffer.getDouble();
        LOGGER.fine(() -> "Read Double: " + value);
        return value;
      };
      case STRING -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker != Constants.STRING.marker()) {
          throw new IllegalStateException("Expected STRING marker but got: " + marker);
        }
        int length = ZigZagEncoding.getInt(buffer);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        String value = new String(bytes, UTF_8);
        LOGGER.fine(() -> "Read String: " + value);
        return value;
      };
      case UUID -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker != Constants.UUID.marker()) {
          throw new IllegalStateException("Expected UUID marker but got: " + marker);
        }
        final long mostSigBits = buffer.getLong();
        final long leastSigBits = buffer.getLong();
        UUID value = new UUID(mostSigBits, leastSigBits);
        LOGGER.fine(() -> "Read UUID: " + value);
        return value;
      };
      case ENUM -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        assert marker == Constants.ENUM.marker() : "Expected ENUM marker but got: " + marker;
        int wireOrdinal = ZigZagEncoding.getInt(buffer);
        int ordinal = wireOrdinal - 1;
        Class<?> enumClass = userTypes[ordinal];
        assert enumClass.equals(leafTag.type()) : "Expected enum type " + leafTag.type() + " but got " + enumClass;

        // Read signature
        long signature = buffer.getLong();

        // Read enum constant ordinal
        int enumOrdinal = ZigZagEncoding.getInt(buffer);

        Object[] enumConstants = enumClass.getEnumConstants();
        assert enumConstants != null : "Not an enum class: " + enumClass;
        assert enumOrdinal >= 0 && enumOrdinal < enumConstants.length : "Invalid enum ordinal: " + enumOrdinal;

        return enumConstants[enumOrdinal];
      };
      case RECORD -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker != Constants.RECORD.marker()) {
          throw new IllegalStateException("Expected RECORD marker but got: " + marker);
        }

        // Read ordinal (1-indexed on wire)
        int wireOrdinal = ZigZagEncoding.getInt(buffer);
        int ordinal = wireOrdinal - 1;

        if (ordinal < 0 || ordinal >= userTypes.length) {
          throw new IllegalStateException("Invalid record ordinal: " + ordinal);
        }

        // Read signature
        final long signature = buffer.getLong();

        // In DEFAULTED mode, handle signature mismatch by using defaults for missing fields
        if (COMPATIBILITY_MODE == CompatibilityMode.DEFAULTED) {
          final long expectedSignature = typeSignatures[ordinal];
          if (expectedSignature != signature) {
            // Schema evolution detected - use backwards compatibility
            return deserializeRecordWithDefaults(buffer, ordinal);
          }
        }

        return deserializeRecord(buffer, ordinal);
      };
      case INTERFACE -> buffer -> {
        // Interface reader that handles both ENUM and RECORD markers
        int marker = ZigZagEncoding.getInt(buffer);

        if (marker == Constants.ENUM.marker()) {
          // This is an enum implementing the sealed interface
          int wireOrdinal = ZigZagEncoding.getInt(buffer);
          int ordinal = wireOrdinal - 1;
          Class<?> enumClass = userTypes[ordinal];

          // Read signature
          long signature = buffer.getLong();

          // Read enum constant ordinal
          int enumOrdinal = ZigZagEncoding.getInt(buffer);

          Object[] enumConstants = enumClass.getEnumConstants();
          assert enumConstants != null : "Not an enum class: " + enumClass;
          assert enumOrdinal >= 0 && enumOrdinal < enumConstants.length : "Invalid enum ordinal: " + enumOrdinal;

          return enumConstants[enumOrdinal];
        } else if (marker == Constants.RECORD.marker()) {
          // This is a record implementing the sealed interface
          int wireOrdinal = ZigZagEncoding.getInt(buffer);
          int ordinal = wireOrdinal - 1;

          if (ordinal < 0 || ordinal >= userTypes.length) {
            throw new IllegalStateException("Invalid record ordinal: " + ordinal);
          }

          // Read signature
          final long signature = buffer.getLong();

          // In DEFAULTED mode, handle signature mismatch by using defaults for missing fields
          if (COMPATIBILITY_MODE == CompatibilityMode.DEFAULTED) {
            final long expectedSignature = typeSignatures[ordinal];
            if (expectedSignature != signature) {
              // Schema evolution detected - use backwards compatibility
              return deserializeRecordWithDefaults(buffer, ordinal);
            }
          }

          return deserializeRecord(buffer, ordinal);
        } else {
          throw new IllegalStateException("Expected RECORD or ENUM marker for interface but got: " + marker);
        }
      };
      default -> throw new IllegalArgumentException("No leaf reader for tag: " + leafTag);
    };
  }

  /// Create reader for Optional values
  Function<ByteBuffer, Object> createOptionalReader(Function<ByteBuffer, Object> elementReader) {
    return buffer -> {
      int marker = ZigZagEncoding.getInt(buffer);
      if (marker == Constants.OPTIONAL_EMPTY.marker()) {
        LOGGER.fine(() -> "Reading OPTIONAL_EMPTY");
        return Optional.empty();
      } else if (marker == Constants.OPTIONAL_OF.marker()) {
        Object value = elementReader.apply(buffer);
        LOGGER.fine(() -> "Reading OPTIONAL_OF with value: " + value);
        return Optional.of(value);
      } else {
        throw new IllegalStateException("Expected OPTIONAL marker but got: " + marker);
      }
    };
  }

  /// Create reader for Map values
  Function<ByteBuffer, Object> createMapReader(Function<ByteBuffer, Object> keyReader, Function<ByteBuffer, Object> valueReader) {
    return buffer -> {
      int marker = ZigZagEncoding.getInt(buffer);
      assert marker == Constants.MAP.marker() : "Expected MAP marker but got: " + marker;

      int size = ZigZagEncoding.getInt(buffer);
      LOGGER.fine(() -> "Reading Map with " + size + " entries");

      Map<Object, Object> map = new HashMap<>(size);
      IntStream.range(0, size).forEach(i -> {
        Object key = keyReader.apply(buffer);
        Object value = valueReader.apply(buffer);
        map.put(key, value);
      });
      return map;
    };
  }

  /// Create reader for List values
  Function<ByteBuffer, Object> createListReader(Function<ByteBuffer, Object> elementReader) {
    return buffer -> {
      int marker = ZigZagEncoding.getInt(buffer);
      assert marker == Constants.LIST.marker() : "Expected LIST marker but got: " + marker;

      int size = buffer.getInt();
      LOGGER.fine(() -> "Reading List with " + size + " elements");
      List<Object> list = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        list.add(elementReader.apply(buffer));
      }
      return Collections.unmodifiableList(list);
    };
  }


  ToIntFunction<Object> buildSizerChain(TypeStructure typeStructure, MethodHandle accessor) {
    // Build the type-specific sizer
    ToIntFunction<Object> typeSizer = buildTypeSizerChain(typeStructure);

    // Extract component value then delegate to type-specific sizer
    return record -> {
      try {
        Object componentValue = accessor.invokeWithArguments(record);
        return componentValue == null ? 1 : typeSizer.applyAsInt(componentValue);
      } catch (Throwable e) {
        throw new RuntimeException("Failed to size component", e);
      }
    };
  }

  /// Build type-specific sizer chain based on TypeStructure
  /// This creates a chain of sizers for nested types (e.g., List<Optional<String>>).
  /// The ordinal parameter passed to createLeafSizer is only used by RECORD and ENUM cases.
  /// For INTERFACE types, the ordinal is ignored - the runtime type will be determined
  /// when sizing the actual object (which will always be a concrete record or enum).
  ///
  /// This is the core recursive metaprogramming method that builds type-specific sizer chains.
  /// It processes the TypeStructure from right-to-left, creating a nested chain of sizers where
  /// each container delegates to its component type's sizer.
  ///
  /// The recursion enables handling of arbitrary nesting depth - the metaprogramming pattern
  /// automatically handles any combination of containers and leaf types.
  ToIntFunction<Object> buildTypeSizerChain(TypeStructure structure) {
    List<TagWithType> tags = structure.tagTypes();
    if (tags.isEmpty()) {
      throw new IllegalArgumentException("Type structure must have at least one tag");
    }

    // TODO: this is a code smell. we need to refactor to have a createLeafSizerUserType and createLeafSizerBuiltInType
    // and make those DRY and we only need to resolve the ordinal in the createLeafSizerUserType
    int ordinal = 0;

    // For complex types, build chain from right to left
    Iterator<TagWithType> reversedTags = tags.reversed().iterator();
    List<ToIntFunction<Object>> sizers = new ArrayList<>(tags.size());

    TagWithType rightmostTag = reversedTags.next();
    ToIntFunction<Object> sizer = createLeafSizer(ordinal, rightmostTag);
    sizers.add(sizer);

    while (reversedTags.hasNext()) {
      final ToIntFunction<Object> innerSizer = sizer;
      TagWithType tag = reversedTags.next();
      sizer = switch (tag.tag()) {
        case LIST -> createListSizer(innerSizer);
        case OPTIONAL -> createOptionalSizer(innerSizer);
        case ARRAY -> createArraySizer(innerSizer);
        case MAP -> {
          // MAP structure: [MAP, key_tags..., value_tags...]
          // Keys must be value types (single tag), but values can be complex
          // When building in reverse order, we build value chain first, then key
          // The key sizer is in innerSizer, value sizer root is at sizers.size() - 2
          final var keySizer = innerSizer;
          final var valueSizer = sizers.get(sizers.size() - 2);
          yield createMapSizer(keySizer, valueSizer);
        }
        default -> createLeafSizer(ordinal, tag);
      };
      sizers.add(sizer);
    }

    return sizer;
  }

  /// Create leaf sizer for primitive/basic types
  ToIntFunction<Object> createLeafSizer(int ordinal, TagWithType leafTag) {
    return switch (leafTag.tag()) {
      case BOOLEAN -> obj -> 1 + 1; // marker + boolean byte
      case BYTE -> obj -> 1 + 1; // marker + byte
      case SHORT -> obj -> 1 + Short.BYTES;
      case CHARACTER -> obj -> 1 + Character.BYTES;
      case INTEGER -> obj -> {
        Integer value = (Integer) obj;
        int zigzagSize = ZigZagEncoding.sizeOf(value);
        return 1 + (Math.min(zigzagSize, Integer.BYTES));
      };
      case LONG -> obj -> {
        Long value = (Long) obj;
        int zigzagSize = ZigZagEncoding.sizeOf(value);
        return 1 + (Math.min(zigzagSize, Long.BYTES));
      };
      case FLOAT -> obj -> 1 + Float.BYTES;
      case DOUBLE -> obj -> 1 + Double.BYTES;
      case STRING -> obj -> {
        if (obj == null) return 1; // NULL marker
        String s = (String) obj;
        byte[] bytes = s.getBytes(UTF_8);
        return 1 + ZigZagEncoding.sizeOf(bytes.length) + bytes.length;
      };
      case UUID -> obj -> 1 + 2 * Long.BYTES;
      case ENUM ->
          obj -> 1 + ZigZagEncoding.sizeOf(1) + CLASS_SIG_BYTES + ZigZagEncoding.sizeOf(((Enum<?>) obj).ordinal()); // marker + class ordinal + signature + enum ordinal
      case RECORD -> obj -> {
        Record record = (Record) obj;
        return 1 + ZigZagEncoding.sizeOf(ordinal + 1) + CLASS_SIG_BYTES + maxSizeOfRecordComponents(record, ordinal);
      };
      case INTERFACE -> obj -> {
        // Interface sizer for sealed interfaces - must be fast approximation, no map lookups
        // We use worst-case Integer.BYTES for ordinal size to avoid expensive lookups
        if (obj instanceof Enum<?> enumValue) {
          // Size for enum: marker + ordinal (worst case) + signature + enum ordinal (worst case)
          return 1 + Integer.BYTES + CLASS_SIG_BYTES + Integer.BYTES;
        } else if (obj instanceof Record record) {
          // Size for record: marker + ordinal (worst case) + signature + components
          // We need the actual record ordinal to size components correctly
          int recordOrdinal = classToTypeInfo.get(record.getClass()).typeOrdinal();
          return 1 + Integer.BYTES + CLASS_SIG_BYTES + maxSizeOfRecordComponents(record, recordOrdinal);
        } else {
          throw new IllegalArgumentException("Unexpected type for interface sizer: " + obj.getClass());
        }
      };
      default -> throw new IllegalArgumentException("No leaf sizer for tag is it a container tag? " + leafTag);
    };
  }

  /// Create sizer for Optional values
  ToIntFunction<Object> createOptionalSizer
  (ToIntFunction<Object> elementSizer) {
    return obj -> {
      Optional<?> optional = (Optional<?>) obj;
      // OPTIONAL_EMPTY=marker, OPTIONAL_OF=marker+sizeOf(element)
      return optional.map(o -> 1 + elementSizer.applyAsInt(o)).orElse(1);
    };
  }

  /// Create sizer for List values
  ToIntFunction<Object> createListSizer(ToIntFunction<Object> elementSizer) {
    return obj -> {
      List<?> list = (List<?>) obj;
      int size = 1 + 4; // LIST marker + size int
      return size + list.stream().mapToInt(elementSizer).sum();
    };
  }

  /// Create sizer for Map values
  ToIntFunction<Object> createMapSizer(ToIntFunction<Object> keySizer, ToIntFunction<Object> valueSizer) {
    return obj -> {
      Map<?, ?> map = (Map<?, ?>) obj;
      int size = ZigZagEncoding.sizeOf(Constants.MAP.marker()) + ZigZagEncoding.sizeOf(map.size()); // MAP marker + size
      return size + map.entrySet().stream()
          .mapToInt(entry -> keySizer.applyAsInt(entry.getKey()) + valueSizer.applyAsInt(entry.getValue()))
          .sum();
    };
  }

  /// Create sizer for Array values with element delegation
  ToIntFunction<Object> createArraySizer(ToIntFunction<Object> elementSizer) {
    return obj -> {
      if (obj == null) {
        LOGGER.finer(() -> "Sizing null array");
        return 1; // NULL marker only
      }
      int length = Array.getLength(obj);
      LOGGER.finer(() -> "Sizing array of length " + length);
      int size = 1 + 1 + ZigZagEncoding.sizeOf(length); // ARRAY marker + element type marker + length
      return size + IntStream.range(0, length)
          .mapToObj(i -> Array.get(obj, i))
          .mapToInt(elementSizer)
          .sum();
    };
  }

  @Override
  public int serialize(ByteBuffer buffer, T object) {
    Objects.requireNonNull(buffer, "buffer cannot be null");
    Objects.requireNonNull(object, "object cannot be null");
    buffer.order(ByteOrder.BIG_ENDIAN);
    final var startPosition = buffer.position();

    // Find ordinal for the root object's class using the ONE HashMap lookup
    final TypeInfo typeInfo = classToTypeInfo.get(object.getClass());
    if (typeInfo == null) {
      throw new IllegalArgumentException("Unknown class: " + object.getClass().getName());
    }
    final int ordinal = typeInfo.typeOrdinal();
    final long typeSignature = typeInfo.typeSignature();
    LOGGER.finer(() -> "Serializing ordinal " + ordinal + " (" + object.getClass().getSimpleName() + ") wire=" + (ordinal + 1));

    // Write ordinal marker (1-indexed on wire)
    ZigZagEncoding.putInt(buffer, ordinal + 1);

    // Serialize based on actual runtime user type
    if (object instanceof Record record) {
      LOGGER.finer(() -> "Writing signature 0x" + Long.toHexString(typeSignature) + " for " + record.getClass().getSimpleName());
      buffer.putLong(typeSignature);
      serializeRecordComponents(buffer, record, typeInfo);
    } else if (object instanceof Enum<?> enumValue) {
      // Write enum signature then ordinal
      LOGGER.finer(() -> "Writing enum signature 0x" + Long.toHexString(typeSignature) + " for " + enumValue.getClass().getSimpleName());
      buffer.putLong(typeSignature);
      LOGGER.finer(() -> "Writing enum ordinal " + enumValue.ordinal() + " for " + enumValue.name());
      ZigZagEncoding.putInt(buffer, enumValue.ordinal());
    } else {
      throw new IllegalArgumentException("Expected Record or Enum but got: " + object.getClass().getName());
    }

    final var totalBytes = buffer.position() - startPosition;
    LOGGER.finer(() -> "PicklerImpl.serialize: completed, totalBytes=" + totalBytes);
    return totalBytes;
  }


  @Override
  public T deserialize(ByteBuffer buffer) {
    Objects.requireNonNull(buffer, "buffer cannot be null");
    buffer.order(ByteOrder.BIG_ENDIAN);
    LOGGER.finer(() -> "PicklerImpl.deserialize: starting at position=" + buffer.position());

    // Read ordinal marker (1-indexed on wire, convert to 0-indexed for array access)
    final int wireOrdinal = ZigZagEncoding.getInt(buffer);
    final int ordinal = wireOrdinal - 1;

    LOGGER.finer(() -> "Deserializing ordinal " + ordinal + " (wire=" + wireOrdinal + ")");

    if (ordinal < 0 || ordinal >= userTypes.length) {
      throw new IllegalStateException("Invalid ordinal: " + ordinal + " (wire=" + wireOrdinal + ")");
    }

    Class<?> targetClass = userTypes[ordinal];
    LOGGER.finer(() -> "Target class: " + targetClass.getSimpleName());

    // Deserialize based on target class type
    if (targetClass.isRecord()) {
      // Read signature
      final long signature = buffer.getLong();

      // In DEFAULTED mode, handle signature mismatch by using defaults for missing fields
      if (COMPATIBILITY_MODE == CompatibilityMode.DEFAULTED) {
        final long expectedSignature = typeSignatures[ordinal];
        if (expectedSignature != signature) {
          // Schema evolution detected - use backwards compatibility
          return deserializeRecordWithDefaults(buffer, ordinal);
        }
      }

      return deserializeRecord(buffer, ordinal);
    } else if (targetClass.isEnum()) {
      // Deserialize enum
      // Read signature
      long signature = buffer.getLong();

      // Read enum constant ordinal
      int enumOrdinal = ZigZagEncoding.getInt(buffer);

      Object[] enumConstants = targetClass.getEnumConstants();
      assert enumConstants != null : "Not an enum class: " + targetClass;
      assert enumOrdinal >= 0 && enumOrdinal < enumConstants.length : "Invalid enum ordinal: " + enumOrdinal;

      //noinspection unchecked
      return (T) enumConstants[enumOrdinal];
    } else {
      throw new IllegalArgumentException("Expected Record or Enum class but got: " + targetClass.getName());
    }
  }

  /// Compute the worst case maximum size for the given object without check whether advanced compaction techniques can be applied.
  /// The estimate will still be less JDK Serializer size, but may be significantly larger than the actual size in some narrow cases.
  /// @throws IllegalArgumentException if the object is not a Record or Enum.
  @Override
  public int maxSizeOf(T object) {
    Objects.requireNonNull(object, "object cannot be null");

    // Handle arrays at root level
    if (!object.getClass().isEnum() && !object.getClass().isRecord()) {
      throw new IllegalArgumentException("Expected Record or Enum object but got: " + object.getClass().getName());
    }

    // Find ordinal for the object's class using the ONE HashMap lookup
    final int ordinal = classToTypeInfo.get(object.getClass()).typeOrdinal();

    // Size = ordinal marker + type-specific content size
    int size = ZigZagEncoding.sizeOf(ordinal + 1); // 1-indexed on wire

    if (object instanceof Record record) {
      size += CLASS_SIG_BYTES; // signature bytes
      size += maxSizeOfRecordComponents(record, ordinal);
    } else if (object instanceof Enum<?> enumValue) {
      // Size for enum signature + ordinal
      size += CLASS_SIG_BYTES; // signature bytes
      size += ZigZagEncoding.sizeOf(enumValue.ordinal()); // enum ordinal
    }

    return size;
  }

  /// Check if an array type contains user-defined types (records, enums)
  static boolean isUserDefinedArrayType(Class<?> arrayType) {
    if (!arrayType.isArray()) return false;
    Class<?> componentType = arrayType.getComponentType();
    // Keep going down for multi-dimensional arrays
    while (componentType.isArray()) {
      componentType = componentType.getComponentType();
    }
    return componentType.isRecord() || componentType.isEnum();
  }

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


}

record TagWithType(Tag tag, Class<?> type) {
}

/// TypeStructure record for analyzing generic types
record TypeStructure(List<TagWithType> tagTypes) {

  static final int MAP_TYPE_ARG_COUNT = 2;

  TypeStructure(List<Tag> tags, List<Class<?>> types) {
    this(IntStream.range(0, Math.min(tags.size(), types.size()))
        .mapToObj(i -> new TagWithType(tags.get(i), types.get(i)))
        .toList());
    if (tags.size() != types.size()) {
      throw new IllegalArgumentException("Tags and types lists must have same size: tags=" + tags.size() + " types=" + types.size() +
          " tags=" + tags + " types=" + types);
    }
  }

  /// Analyze a generic type and extract its structure
  ///
  /// This method is the entry point for the recursive metaprogramming pattern. It performs
  /// static analysis on a Java Type to decompose it into a chain of container types
  /// followed by a leaf type.
  ///
  /// The analysis recursively unwraps:
  /// - Generic types (List<T>, Map<K,V>, Optional<T>)
  /// - Array types (T[], T[][], etc.)
  /// - Raw types to their component types
  ///
  /// The result is a TypeStructure containing a list like:
  /// [LIST, ARRAY, MAP, RECORD] for a type like List<String[][]>
  ///
  /// This static analysis enables the builder methods (buildWriterChain, buildReaderChain,
  /// buildSizerChain) to construct appropriate delegation chains at construction time,
  /// avoiding any runtime type inspection during serialization/deserialization.
  ///
  /// The pattern supports arbitrary nesting depth - e.g., List<Map<String, Optional<Integer[]>[]>>
  /// is fully supported without any special-case code.
  static TypeStructure analyze(Type type) {
    List<Tag> tags = new ArrayList<>();
    List<Class<?>> types = new ArrayList<>();

    Object current = type;

    while (current != null) {
      switch (current) {
        case ParameterizedType paramType -> {
          Type rawType = paramType.getRawType();

          if (rawType.equals(List.class)) {
            tags.add(LIST);
            types.add(List.class); // Container class for symmetry with Arrays.class pattern
            Type[] typeArgs = paramType.getActualTypeArguments();
            current = typeArgs.length > 0 ? typeArgs[0] : null;
          } else if (rawType.equals(Map.class)) {
            tags.add(MAP);
            types.add(Map.class); // Container class for symmetry
            Type[] typeArgs = paramType.getActualTypeArguments();
            if (typeArgs.length == MAP_TYPE_ARG_COUNT) {
              // For Map<K,V>, we need to analyze both key and value types
              // Store Map marker, then key structure, then value structure
              TypeStructure keyStructure = analyze(typeArgs[0]);
              TypeStructure valueStructure = analyze(typeArgs[1]);

              // Combine: [MAP, ...key tags, ...value tags]
              for (TagWithType tt : keyStructure.tagTypes()) {
                tags.add(tt.tag());
                types.add(tt.type());
              }
              for (TagWithType tt : valueStructure.tagTypes()) {
                tags.add(tt.tag());
                types.add(tt.type());
              }
            }
            return new TypeStructure(tags, types);
          } else if (rawType.equals(Optional.class)) {
            tags.add(OPTIONAL);
            types.add(Optional.class); // Container class for symmetry
            Type[] typeArgs = paramType.getActualTypeArguments();
            current = typeArgs.length > 0 ? typeArgs[0] : null;
          } else {
            // Unknown parameterized type, treat as raw type
            current = rawType;
          }
        }
        case GenericArrayType arrayType -> {
          // Handle arrays of parameterized types like Optional<String>[]
          tags.add(ARRAY);
          types.add(Arrays.class); // Arrays.class as marker

          current = arrayType.getGenericComponentType(); // Continue processing element type
        }
        case Class<?> clazz -> {
          if (clazz.isArray()) {
            // Array container with element type, e.g. short[] -> [ARRAY, SHORT]
            tags.add(ARRAY);
            types.add(Arrays.class); // Arrays.class as marker, not concrete array type
            current = clazz.getComponentType(); // Continue processing element type
          } else {
            // Regular class - terminal case
            tags.add(fromClass(clazz));
            types.add(clazz);
            return new TypeStructure(tags, types);
          }
        }
        default -> {
          // Unknown type, return what we have
          return new TypeStructure(tags, types);
        }
      }
    }

    return new TypeStructure(tags, types);
  }
}
