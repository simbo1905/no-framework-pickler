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
public sealed interface Pickler<T> permits CustomSerde, EmptyRecordSerde, EnumSerde, ManySerde, RecordSerde {

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
    return forClass(clazz, Map.of(), List.of());
  }

  /// Factory method for creating a unified pickler for any type using refactored architecture
  /// @param clazz The root class (record, enum, or sealed interface)
  /// @param typeSignatures A map of class signatures to type signatures for backwards compatibility
  /// @return A pickler instance
  static <T> Pickler<T> forClass(
      Class<T> clazz, Map<Class<?>, Long> typeSignatures) {
    return forClass(clazz, typeSignatures, List.of());
  }

  /// Factory method for creating a unified pickler for any type using refactored architecture
  /// @param clazz The root class (record, enum, or sealed interface)
  /// @param typeSignatures A map of class signatures to type signatures for backwards compatibility
  /// @param customHandlers A list of custom handlers for value-based types
  /// @return A pickler instance
  static <T> Pickler<T> forClass(
      Class<T> clazz, Map<Class<?>, Long> typeSignatures,
      List<SerdeHandler> customHandlers
  ) {
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
    final var recordClassHierarchy = recordClassHierarchy(clazz, customHandlers);

    // As classes may be renamed or moved, we need to ensure that the type signatures map given by the user is a reachable class
    typeSignatures.keySet().stream().filter(k -> !recordClassHierarchy.contains(k))
        .findAny()
        .ifPresent(k -> {
          throw new IllegalArgumentException("Type signatures map must not contain a class that is not in the record " +
              "hierarchy for class " + clazz + " was given as a key: " + k + " which is not one of " +
              recordClassHierarchy.stream().map(Class::getName).collect(Collectors.joining(",")));
        });

    // Create a set of custom handler types for efficient lookup
    final var customHandlerTypes = customHandlers.stream()
        .map(SerdeHandler::valueBasedLike)
        .collect(Collectors.toSet());

    // Partition classes into legal and illegal
    final Map<Boolean, List<Class<?>>> legalAndIllegalClasses = recordClassHierarchy.stream()
        .collect(Collectors.partitioningBy(cls ->
            customHandlerTypes.contains(cls) || // <-- Add this line
                cls.isRecord() || cls.isEnum() || cls.isSealed() || cls.isArray() || cls.isPrimitive() ||
                String.class.equals(cls) || LocalDate.class.isAssignableFrom(cls) || LocalDateTime.class.isAssignableFrom(cls) ||
                List.class.isAssignableFrom(cls) || Optional.class.isAssignableFrom(cls) ||
                Map.class.isAssignableFrom(cls) || BOXED_PRIMITIVES.contains(cls)
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

    // Get all classes that need picklers (records and enums)
    final var allPicklerClasses = legalAndIllegalClasses.get(Boolean.TRUE).stream()
        .filter(cls -> cls.isRecord() || cls.isEnum())
        .collect(Collectors.toSet());

    // Build enum-to-signature mapping
    @SuppressWarnings("unchecked") final Map<Class<Enum<?>>, Long> enumToTypeSignatureMap = legalAndIllegalClasses.get(Boolean.TRUE).stream()
        .filter(Class::isEnum)
        .map(cls -> (Class<Enum<?>>) cls)
        .filter(Enum.class::isAssignableFrom)
        .collect(Collectors.toMap(
            enumClass -> enumClass,
            Companion::hashEnumSignature
        ));

    final var dependencies = analyzeDependencies(new HashSet<>(recordClasses), customHandlers);

    // Check for empty record first
    if (clazz.isRecord() && clazz.getRecordComponents().length == 0) {
      LOGGER.fine(() -> "Creating EmptyRecordSerde for empty record: " + clazz.getName());
      final Long typeSignature = typeSignatures.getOrDefault(clazz,
          Companion.hashClassSignature(clazz, new RecordComponent[0], new TypeExpr[0]));
      final Optional<Long> altTypeSignature = Optional.empty();
      return new EmptyRecordSerde<>(clazz, typeSignature, altTypeSignature);
    } else if (clazz.isEnum()) {
      LOGGER.fine(() -> "Creating EnumSerde for enum: " + clazz.getName());
      final Long typeSignature = typeSignatures.getOrDefault(clazz,
          Companion.hashEnumSignature(clazz));
      final Optional<Long> altTypeSignature = Optional.empty();
      @SuppressWarnings("rawtypes") final var enumClass = (Class) clazz;
      @SuppressWarnings({"unchecked", "rawtypes"}) final Pickler<T> enumPickler = new EnumSerde(enumClass, typeSignature, altTypeSignature);
      return enumPickler;
    } else if (recordClasses.size() == 1 && dependencies.getOrDefault(clazz, Set.of()).isEmpty() && clazz.isRecord()) {
      // Simple case: single record with no record/enum dependencies
      LOGGER.fine(() -> "Creating RecordSerde for simple record: " + clazz.getName());
      return createDirectRecordSerde(clazz, typeSignatures, enumToTypeSignatureMap, customHandlers);
    } else {
      // Complex case: multiple records or dependencies require ManySerde
      LOGGER.fine(() -> "Creating ManySerde for complex record: " + clazz.getName());
      return createManySerde(clazz, allPicklerClasses, typeSignatures, customHandlers);
    }
  }

  /// Analyze dependencies between record types to determine delegation needs
  private static Map<Class<?>, Set<Class<?>>> analyzeDependencies(Set<Class<?>> recordClasses, Collection<SerdeHandler> customHandlers) {
    final var dependencies = new HashMap<Class<?>, Set<Class<?>>>();

    for (Class<?> recordClass : recordClasses) {
      final var dependencySet = Arrays.stream(recordClass.getRecordComponents())
          .map(component -> TypeExpr.analyzeType(component.getGenericType(), customHandlers))
          .flatMap(typeExpr -> classesInAST(typeExpr).stream())
          .filter(recordClasses::contains) // Only dependencies within our record hierarchy
          .collect(Collectors.toSet());

      dependencies.put(recordClass, dependencySet);
    }

    return dependencies;
  }

  /// Extract all classes referenced in a TypeExpr AST
  private static Set<Class<?>> classesInAST(TypeExpr typeExpr) {
    final var classes = new HashSet<Class<?>>();

    switch (typeExpr) {
      case TypeExpr.ArrayNode(var element, var componentType) -> {
        classes.add(componentType);
        classes.addAll(classesInAST(element));
      }
      case TypeExpr.ListNode(var element) -> classes.addAll(classesInAST(element));
      case TypeExpr.OptionalNode(var wrapped) -> classes.addAll(classesInAST(wrapped));
      case TypeExpr.MapNode(var key, var value) -> {
        classes.addAll(classesInAST(key));
        classes.addAll(classesInAST(value));
      }
      case TypeExpr.RefValueNode(var ignored, var javaType) -> {
        if (javaType instanceof Class<?> cls) {
          classes.add(cls);
        }
      }
      case TypeExpr.PrimitiveValueNode(var ignored, var javaType) -> {
        if (javaType instanceof Class<?> cls) {
          classes.add(cls);
        }
      }
      case TypeExpr.PrimitiveArrayNode(var ignored, var arrayType) -> classes.add(arrayType);
    }

    return classes;
  }

  /// Create RecordSerde
  private static <T> RecordSerde<T> createDirectRecordSerde(
      Class<T> userType, Map<Class<?>, Long> typeSignatures,
      Map<Class<Enum<?>>, Long> enumToTypeSignatureMap,
      List<SerdeHandler> customHandlers
  ) {
    final var recordClasses = List.<Class<?>>of(userType);
    final var recordTypeSignatures = Companion.computeRecordTypeSignatures(recordClasses, customHandlers);
    final var typeSignature = recordTypeSignatures.get(userType);
    final var altSignature = Optional.ofNullable(typeSignatures.get(userType));

    // Pre-process custom handlers into efficient lookup maps
    final Map<Class<?>, Serde.Sizer> customSizers = customHandlers.stream()
        .collect(Collectors.toMap(SerdeHandler::valueBasedLike, SerdeHandler::sizer));
    final Map<Class<?>, Serde.Writer> customWriters = customHandlers.stream()
        .collect(Collectors.toMap(SerdeHandler::valueBasedLike, SerdeHandler::writer));
    final Map<Class<?>, Serde.Reader> customReaders = customHandlers.stream()
        .collect(Collectors.toMap(SerdeHandler::valueBasedLike, SerdeHandler::reader));

    // 1. Create a resolver specifically for the direct build path
    final Companion.DependencyResolver resolver = new Companion.DependencyResolver() {
      @Override
      public Pickler<?> resolve(Class<?> clazz) {
        if (clazz.isEnum()) {
          @SuppressWarnings({"rawtypes"}) final var enumClass = (Class) clazz;
          final var enumTypeSignature = enumToTypeSignatureMap.get(enumClass);
          @SuppressWarnings({"unchecked", "rawtypes"})
          Pickler<?> r = new EnumSerde(enumClass, enumTypeSignature, Optional.empty());
          return r;
        }
        // Handle custom types if necessary
        if (customReaders.containsKey(clazz)) {
          // This class is a custom type. Create a simple pickler for it.
          final Serde.Sizer sizer = customSizers.get(clazz);
          final Serde.Writer writer = customWriters.get(clazz);
          final Serde.Reader reader = customReaders.get(clazz);

          // Return a new anonymous Pickler that wraps the custom handler's logic.
          return new CustomSerde<>(sizer, writer, reader);
        }
        // In a direct build, we don't expect to resolve other records.
        throw new UnsupportedOperationException("Direct resolver cannot resolve class: " + clazz.getName());
      }

      @Override
      public Pickler<?> resolveBySignature(long typeSignature) {
        // This is primarily for deserializing interfaces/records from a stream,
        // which is not the main concern for the direct writer/sizer setup.
        // However, a reader for a component might need it.
        final var handler = customHandlers.stream().filter(h -> h.marker() == typeSignature).findFirst();
        if (handler.isPresent()) {
          final var h = handler.get();
          return new CustomSerde<>(h.sizer(), h.writer(), h.reader());
        } else {
          throw new UnsupportedOperationException("Signature resolution not supported in direct build path: " + Long.toHexString(typeSignature));
        }
      }
    };

    // 2. Call the new, unified component builder method
    final var componentSerdes = Companion.buildComponentSerdesWithCallback(
        userType,
        resolver,
        customHandlers
    );

    final var sizers = Arrays.stream(componentSerdes).map(ComponentSerde::sizer).toArray(Serde.Sizer[]::new);
    final var writers = Arrays.stream(componentSerdes).map(ComponentSerde::writer).toArray(Serde.Writer[]::new);
    final var readers = Arrays.stream(componentSerdes).map(ComponentSerde::reader).toArray(Serde.Reader[]::new);

    return new RecordSerde<>(userType, typeSignature, altSignature, sizers, writers, readers);
  }

  /// Create ManySerde for complex cases with delegation
  private static <T> ManySerde<T> createManySerde(
      Class<T> rootClass,
      Set<Class<?>> allClasses,
      Map<Class<?>, Long> typeSignatures, List<SerdeHandler> customHandlers) {

    // Create the shared resolver map that all instances will use
    final var serdes = new HashMap<Class<?>, Pickler<?>>();
    final var typeSignatureToSerde = new HashMap<Long, Pickler<?>>();

    // Compute type signatures for all record classes
    final var recordClasses = allClasses.stream()
        .filter(Class::isRecord)
        .toList();
    final var recordTypeSignatures = Companion.computeRecordTypeSignatures(recordClasses, customHandlers);

    // Create dependency resolver callback with type signature resolution
    // FIXME this can be a functional interface in ManySerde and we can use a lambda directly
    final Companion.DependencyResolver resolver = new Companion.DependencyResolver() {
      @Override
      public Pickler<?> resolve(Class<?> targetClass) {
        final var serde = serdes.get(targetClass);
        if (serde == null) {
          throw new IllegalStateException("No serde found for class: " + targetClass);
        }
        return serde;
      }

      @Override
      public Pickler<?> resolveBySignature(long typeSignature) {
        final var serde = typeSignatureToSerde.get(typeSignature);
        if (serde == null) {
          throw new IllegalStateException("No serde found for type signature: 0x" + Long.toHexString(typeSignature));
        }
        return serde;
      }
    };

    // Create EmptyRecordSerde instances first since they have no dependencies
    for (Class<?> recordClass : allClasses) {
      if (recordClass.isRecord() && recordClass.getRecordComponents().length == 0) {
        final var typeSignature = recordTypeSignatures.get(recordClass);
        final var altSignature = Optional.ofNullable(typeSignatures.get(recordClass));
        final var serde = new EmptyRecordSerde<>(recordClass, typeSignature, altSignature);

        serdes.put(recordClass, serde);
        LOGGER.fine(() -> "Registering EmptyRecordSerde " + recordClass.getName() + " with signature 0x" + Long.toHexString(typeSignature));
        if (typeSignatureToSerde.containsKey(typeSignature)) {
          LOGGER.severe(() -> "COLLISION: signature 0x" + Long.toHexString(typeSignature) + " already mapped to " + typeSignatureToSerde.get(typeSignature).getClass().getName());
        }
        typeSignatureToSerde.put(typeSignature, serde);
        altSignature.ifPresent(aLong -> typeSignatureToSerde.put(aLong, serde));
      }
    }

    // Create EnumSerde instances for all enum classes
    for (Class<?> enumClass : allClasses) {
      if (enumClass.isEnum()) {
        LOGGER.fine(() -> "Creating EnumSerde for enum in ManySerde: " + enumClass.getName());
        final var typeSignature = Companion.hashEnumSignature(enumClass);
        final var altSignature = Optional.<Long>empty();
        @SuppressWarnings({"unchecked", "rawtypes"}) final var enumSerde = new EnumSerde(enumClass, typeSignature, altSignature);

        serdes.put(enumClass, enumSerde);
        typeSignatureToSerde.put(typeSignature, enumSerde);
        LOGGER.fine(() -> "Added EnumSerde to serdes map for: " + enumClass.getName() + " with signature: 0x" + Long.toHexString(typeSignature));
      }
    }

    // Build all non-empty RecordSerde instances in single pass using callback
    for (Class<?> recordClass : allClasses) {
      if (recordClass.isRecord() && recordClass.getRecordComponents().length > 0) {
        final var typeSignature = recordTypeSignatures.get(recordClass);
        final var altSignature = Optional.ofNullable(typeSignatures.get(recordClass));
        // Use callback-based component building
        final var componentSerdes = Companion.buildComponentSerdesWithCallback(recordClass, resolver, customHandlers);
        final var sizers = Arrays.stream(componentSerdes).map(ComponentSerde::sizer).toArray(Serde.Sizer[]::new);
        final var writers = Arrays.stream(componentSerdes).map(ComponentSerde::writer).toArray(Serde.Writer[]::new);
        final var readers = Arrays.stream(componentSerdes).map(ComponentSerde::reader).toArray(Serde.Reader[]::new);

        final var serde = new RecordSerde<>(recordClass, typeSignature, altSignature, sizers, writers, readers);
        serdes.put(recordClass, serde);
        LOGGER.fine(() -> "Registering RecordSerde " + recordClass.getName() + " with signature 0x" + Long.toHexString(typeSignature));
        if (typeSignatureToSerde.containsKey(typeSignature)) {
          LOGGER.severe(() -> "COLLISION: signature 0x" + Long.toHexString(typeSignature) + " already mapped to " + typeSignatureToSerde.get(typeSignature).getClass().getName());
        }
        typeSignatureToSerde.put(typeSignature, serde);
        altSignature.ifPresent(aLong -> typeSignatureToSerde.put(aLong, serde));
      }
    }

    return new ManySerde<>(rootClass, serdes, typeSignatureToSerde);
  }

  /// In order to support optional backwards compatibility, we need to be able to tell the newer pickler what is the
  /// type signature of the original class.
  long typeSignature(Class<?> originalClass);
}
