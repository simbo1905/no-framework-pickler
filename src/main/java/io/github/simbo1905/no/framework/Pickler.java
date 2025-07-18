// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//

package io.github.simbo1905.no.framework;

import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static io.github.simbo1905.no.framework.Companion.BOXED_PRIMITIVES;
import static io.github.simbo1905.no.framework.Companion.recordClassHierarchy;


/// Main interface for the No Framework Pickler serialization library.
/// Provides type-safe, reflection-free serialization for records and sealed interfaces.
public sealed interface Pickler<T> permits EmptyRecordSerde, PicklerImpl, RecordSerde {

  Logger LOGGER = Logger.getLogger(Pickler.class.getName());

  /// Serialize an object to a ByteBuffer
  /// @param buffer The buffer to write to
  /// @param record The object to serialize
  /// @return The number of bytes written
  int serialize(ByteBuffer buffer, T record);

  /// Deserialize an object from a ByteBuffer
  /// @param buffer The buffer to read from
  /// @return The deserialized object
  T deserialize(ByteBuffer buffer);

  /// Calculate the maximum size needed to serialize an object
  /// @param record The object to size
  /// @return The maximum number of bytes needed
  int maxSizeOf(T record);

  /// Factory method for creating a unified pickler for any type using refactored architecture
  /// @param clazz The root class (record, enum, or sealed interface)
  /// @return A pickler instance
  static <T> Pickler<T> forClass(Class<T> clazz) {
    return forClass(clazz, Map.of());
  }

  /// Factory method for creating a unified pickler for any type using refactored architecture
  /// @param clazz The root class (record, enum, or sealed interface)
  /// @param typeSignatures A map of class signatures to type signatures for backwards compatibility
  /// @return A pickler instance
  static <T> Pickler<T> forClass(Class<T> clazz, Map<Class<?>, Long> typeSignatures) {
    Objects.requireNonNull(clazz, "Class must not be null");
    if (!clazz.isRecord() && !clazz.isEnum() && !clazz.isSealed()) {
      throw new IllegalArgumentException("Class must be a record, enum, or sealed interface: " + clazz);
    }
    Objects.requireNonNull(typeSignatures, "Type signatures map must not be null");

    // Validate type signatures map as it is user input.
    typeSignatures.keySet().stream().filter(k -> k == null || !k.isRecord())
        .findAny()
        .ifPresent(k -> {
          throw new IllegalArgumentException("Type signatures map must not contain null keys " +
              "or a class that is not a record was given as a key: " + k);
        });

    // Phase 1: Discover all reachable record types
    final var recordClassHierarchy = recordClassHierarchy(clazz);

    // As classes may be renamed or moved, we need to ensure that the type signatures map given by the user is a reachable class
    typeSignatures.keySet().stream().filter(k -> !recordClassHierarchy.contains(k))
        .findAny()
        .ifPresent(k -> {
          throw new IllegalArgumentException("Type signatures map must not contain a class that is not in the record " +
              "hierarchy for class " + clazz + " was given as a key: " + k + " which is not one of " +
              recordClassHierarchy.stream().map(Class::getName).collect(Collectors.joining(",")));
        });

    // Partition classes into legal and illegal
    final Map<Boolean, List<Class<?>>> legalAndIllegalClasses = recordClassHierarchy.stream()
        .collect(Collectors.partitioningBy(cls ->
            cls.isRecord() || cls.isEnum() || cls.isSealed() || cls.isArray() || cls.isPrimitive()
                || String.class.equals(cls) || UUID.class.isAssignableFrom(cls) || LocalDate.class.isAssignableFrom(cls) || LocalDateTime.class.isAssignableFrom(cls)
                || List.class.isAssignableFrom(cls) || Optional.class.isAssignableFrom(cls)
                || Map.class.isAssignableFrom(cls) || BOXED_PRIMITIVES.contains(cls)
        ));

    // Check for illegal classes
    final var illegalClasses = legalAndIllegalClasses.get(Boolean.FALSE);
    if (!illegalClasses.isEmpty()) {
      throw new IllegalArgumentException("Illegal types found: " +
          illegalClasses.stream().map(Class::getName).collect(Collectors.joining(", ")));
    }

    // Filter out record classes that we need to make RecordSerde for
    final var recordClasses = legalAndIllegalClasses.get(Boolean.TRUE).stream()
        .filter(Class::isRecord)
        .toList();
    if (recordClasses.isEmpty()) {
      throw new IllegalArgumentException("No record classes found in hierarchy of: " + clazz);
    }

    // Build enum-to-signature mapping
    @SuppressWarnings("unchecked") final Map<Class<Enum<?>>, Long> enumToTypeSignatureMap = legalAndIllegalClasses.get(Boolean.TRUE).stream()
        .filter(Class::isEnum)
        .map(cls -> (Class<Enum<?>>) cls)
        .filter(Enum.class::isAssignableFrom)
        .collect(Collectors.toMap(
            enumClass -> enumClass,
            Companion::hashEnumSignature
        ));

    final var dependencies = analyzeDependencies(new HashSet<>(recordClasses));

    // Check for empty record first
    if (clazz.isRecord() && clazz.getRecordComponents().length == 0) {
      LOGGER.fine(() -> "Creating EmptyRecordSerde for empty record: " + clazz.getName());
      final Long typeSignature = typeSignatures.getOrDefault(clazz,
          Companion.hashClassSignature(clazz, new RecordComponent[0], new TypeExpr2[0]));
      final Optional<Long> altTypeSignature = Optional.empty();
      return new EmptyRecordSerde<>(clazz, typeSignature, altTypeSignature);
    } else if (recordClasses.size() == 1 && dependencies.getOrDefault(clazz, Set.of()).isEmpty()) {
      // Simple case: single record with no record/enum dependencies
      LOGGER.fine(() -> "Creating RecordSerde for simple record: " + clazz.getName());
      return createSimpleRecordSerde(clazz, typeSignatures, enumToTypeSignatureMap);
    } else {
      // Complex case: multiple records or dependencies require PicklerImpl
      LOGGER.fine(() -> "Creating PicklerImpl for complex record: " + clazz.getName());
      return createPicklerImpl(clazz, new HashSet<>(recordClasses), typeSignatures);
    }
  }

  /// Analyze dependencies between record types to determine delegation needs
  private static Map<Class<?>, Set<Class<?>>> analyzeDependencies(Set<Class<?>> recordClasses) {
    final var dependencies = new HashMap<Class<?>, Set<Class<?>>>();

    for (Class<?> recordClass : recordClasses) {
      final var dependencySet = Arrays.stream(recordClass.getRecordComponents())
          .map(component -> TypeExpr2.analyzeType(component.getGenericType(), List.of()))
          .flatMap(typeExpr -> classesInAST(typeExpr).stream())
          .filter(recordClasses::contains) // Only dependencies within our record hierarchy
          .collect(Collectors.toSet());

      dependencies.put(recordClass, dependencySet);
    }

    return dependencies;
  }

  /// Extract all classes referenced in a TypeExpr2 AST
  private static Set<Class<?>> classesInAST(TypeExpr2 typeExpr) {
    final var classes = new HashSet<Class<?>>();

    switch (typeExpr) {
      case TypeExpr2.ArrayNode(var element, var componentType) -> {
        classes.add(componentType);
        classes.addAll(classesInAST(element));
      }
      case TypeExpr2.ListNode(var element) -> classes.addAll(classesInAST(element));
      case TypeExpr2.OptionalNode(var wrapped) -> classes.addAll(classesInAST(wrapped));
      case TypeExpr2.MapNode(var key, var value) -> {
        classes.addAll(classesInAST(key));
        classes.addAll(classesInAST(value));
      }
      case TypeExpr2.RefValueNode(var ignored, var javaType) -> {
        if (javaType instanceof Class<?> cls) {
          classes.add(cls);
        }
      }
      case TypeExpr2.PrimitiveValueNode(var ignored, var javaType) -> {
        if (javaType instanceof Class<?> cls) {
          classes.add(cls);
        }
      }
      case TypeExpr2.PrimitiveArrayNode(var ignored, var arrayType) -> classes.add(arrayType);
    }

    return classes;
  }

  /// Create sizer resolver that handles enum types
  private static SizerResolver createEnumSizerResolver(Map<Class<Enum<?>>, Long> enumToTypeSignatureMap) {
    return targetClass -> {
      if (targetClass.isEnum() && enumToTypeSignatureMap.containsKey(targetClass)) {
        // Enum size is constant: type signature + worst-case ordinal (no separate null marker needed)
        return obj -> Long.BYTES + Integer.BYTES;
      }
      return null; // Let RecordSerde handle its own types
    };
  }

  /// Create writer resolver that handles enum types
  private static WriterResolver createEnumWriterResolver(Map<Class<Enum<?>>, Long> enumToTypeSignatureMap) {
    return targetClass -> {
      if (targetClass.isEnum() && enumToTypeSignatureMap.containsKey(targetClass)) {
        final Long enumTypeSignature = enumToTypeSignatureMap.get(targetClass);
        return (buffer, obj) -> {
          buffer.putLong(enumTypeSignature);
          if (obj == null) {
            ZigZagEncoding.putInt(buffer, Companion.NULL_MARKER); // -128 as sentinel for null enum
          } else {
            Enum<?> enumValue = (Enum<?>) obj;
            ZigZagEncoding.putInt(buffer, enumValue.ordinal()); // ZigZag encode the ordinal
          }
        };
      }
      return null; // Let RecordSerde handle its own types
    };
  }

  /// Create reader resolver that handles enum types
  private static ReaderResolver createEnumReaderResolver(Map<Class<Enum<?>>, Long> enumToTypeSignatureMap) {
    // Reverse mapping: signature -> enum class
    final Map<Long, Class<Enum<?>>> signatureToEnumClass = enumToTypeSignatureMap.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

    return typeSignature -> {
      final Class<Enum<?>> enumClass = signatureToEnumClass.get(typeSignature);
      if (enumClass != null) {
        return buffer -> {
          final int value = ZigZagEncoding.getInt(buffer);
          if (value < 0) { // Negative value is the NULL_MARKER sentinel
            return null;
          }
          return enumClass.getEnumConstants()[value];
        };
      }
      return null; // Let RecordSerde handle its own types
    };
  }

  /// Create RecordSerde
  private static <T> RecordSerde<T> createSimpleRecordSerde(Class<T> userType, Map<Class<?>, Long> typeSignatures, Map<Class<Enum<?>>, Long> enumToTypeSignatureMap) {
    final var recordClasses = List.<Class<?>>of(userType);
    final var recordTypeSignatures = Companion.computeRecordTypeSignatures(recordClasses);
    final var typeSignature = recordTypeSignatures.get(userType);
    final var altSignature = Optional.ofNullable(typeSignatures.get(userType));

    // Create resolvers that handle enum types
    final SizerResolver sizerResolver = createEnumSizerResolver(enumToTypeSignatureMap);
    final WriterResolver writerResolver = createEnumWriterResolver(enumToTypeSignatureMap);
    final ReaderResolver readerResolver = createEnumReaderResolver(enumToTypeSignatureMap);

    // Create component serdes directly without external resolvers
    final var customHandlers = List.<SerdeHandler>of();
    final var componentSerdes = Companion.buildComponentSerdes(
        userType,
        customHandlers,
        sizerResolver,
        writerResolver,
        readerResolver
    );

    final var sizers = Arrays.stream(componentSerdes).map(ComponentSerde::sizer).toArray(Sizer[]::new);
    final var writers = Arrays.stream(componentSerdes).map(ComponentSerde::writer).toArray(Writer[]::new);
    final var readers = Arrays.stream(componentSerdes).map(ComponentSerde::reader).toArray(Reader[]::new);

    return new RecordSerde<>(userType, typeSignature, altSignature, sizers, writers, readers);
  }

  /// Create PicklerImpl for complex cases with delegation
  private static <T> PicklerImpl<T> createPicklerImpl(
      Class<T> rootClass,
      Set<Class<?>> recordClasses,
      Map<Class<?>, Long> typeSignatures) {

    // Create the shared resolver map that all instances will use
    final var serdes = new HashMap<Class<?>, Pickler<?>>();
    final var typeSignatureToSerde = new HashMap<Long, Pickler<?>>();

    // Compute type signatures for all record classes
    final var recordTypeSignatures = Companion.computeRecordTypeSignatures(recordClasses.stream().toList());

    // Create dependency resolver callback
    final Companion.DependencyResolver resolver = targetClass -> {
      final var serde = serdes.get(targetClass);
      if (serde == null) {
        throw new IllegalStateException("No serde found for class: " + targetClass);
      }
      return serde;
    };

    // Create EmptyRecordSerde instances first since they have no dependencies
    for (Class<?> recordClass : recordClasses) {
      if (recordClass.getRecordComponents().length == 0) {
        final var typeSignature = recordTypeSignatures.get(recordClass);
        final var altSignature = Optional.ofNullable(typeSignatures.get(recordClass));
        final var serde = new EmptyRecordSerde<>(recordClass, typeSignature, altSignature);

        serdes.put(recordClass, serde);
        typeSignatureToSerde.put(typeSignature, serde);
        altSignature.ifPresent(aLong -> typeSignatureToSerde.put(aLong, serde));
      }
    }

    // Build all non-empty RecordSerde instances in single pass using callback
    for (Class<?> recordClass : recordClasses) {
      if (recordClass.getRecordComponents().length > 0) {
        final var typeSignature = recordTypeSignatures.get(recordClass);
        final var altSignature = Optional.ofNullable(typeSignatures.get(recordClass));

        // Use callback-based component building
        final var componentSerdes = Companion.buildComponentSerdesWithCallback(recordClass, resolver);
        final var sizers = Arrays.stream(componentSerdes).map(ComponentSerde::sizer).toArray(Sizer[]::new);
        final var writers = Arrays.stream(componentSerdes).map(ComponentSerde::writer).toArray(Writer[]::new);
        final var readers = Arrays.stream(componentSerdes).map(ComponentSerde::reader).toArray(Reader[]::new);

        final var serde = new RecordSerde<>(recordClass, typeSignature, altSignature, sizers, writers, readers);
        serdes.put(recordClass, serde);
        typeSignatureToSerde.put(typeSignature, serde);
        altSignature.ifPresent(aLong -> typeSignatureToSerde.put(aLong, serde));
      }
    }

    return new PicklerImpl<>(rootClass, serdes, typeSignatureToSerde);
  }

  /// In order to support optional backwards compatibility, we need to be able to tell the newer pickler what is the
  /// type signature of the original class.
  long typeSignature(Class<?> originalClass);
}
