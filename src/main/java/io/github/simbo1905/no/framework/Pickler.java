// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//

package io.github.simbo1905.no.framework;

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

    // Phase 2: Analyze dependencies between record types
    final var dependencies = analyzeDependencies(recordClasses.stream().collect(Collectors.toSet()));

    // Phase 3: Choose architecture based on complexity
    if (recordClasses.size() == 1 && dependencies.get(clazz).isEmpty()) {
      // Simple case: single record with no record/enum dependencies
      return createSimpleRecordSerde(clazz, typeSignatures);
    } else {
      // Complex case: multiple records or dependencies require PicklerImpl
      return createPicklerImpl(clazz, recordClasses.stream().collect(Collectors.toSet()), dependencies, typeSignatures);
    }
  }

  /// Analyze dependencies between record types to determine delegation needs
  private static Map<Class<?>, Set<Class<?>>> analyzeDependencies(Set<Class<?>> recordClasses) {
    final var dependencies = new HashMap<Class<?>, Set<Class<?>>>();

    for (Class<?> recordClass : recordClasses) {
      final var deps = Arrays.stream(recordClass.getRecordComponents())
          .map(component -> TypeExpr2.analyzeType(component.getGenericType(), List.of()))
          .flatMap(typeExpr -> classesInAST(typeExpr).stream())
          .filter(recordClasses::contains) // Only dependencies within our record hierarchy
          .collect(Collectors.toSet());

      dependencies.put(recordClass, deps);
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
      case TypeExpr2.RefValueNode(var type, var javaType) -> {
        if (javaType instanceof Class<?> cls) {
          classes.add(cls);
        }
      }
      case TypeExpr2.PrimitiveValueNode(var type, var javaType) -> {
        if (javaType instanceof Class<?> cls) {
          classes.add(cls);
        }
      }
      case TypeExpr2.PrimitiveArrayNode(var primitiveType, var arrayType) -> classes.add(arrayType);
    }

    return classes;
  }

  /// Create simple RecordSerde for records with no record/enum dependencies
  private static <T> RecordSerde<T> createSimpleRecordSerde(Class<T> clazz, Map<Class<?>, Long> typeSignatures) {
    final var recordClasses = List.<Class<?>>of(clazz);
    final var recordTypeSignatures = Companion.computeRecordTypeSignatures(recordClasses);
    final var typeSignature = recordTypeSignatures.get(clazz);
    final var altSignature = Optional.ofNullable(typeSignatures.get(clazz));

    // No resolvers needed - all components are primitives or built-in types
    return new RecordSerde<>(clazz, typeSignature, altSignature);
  }

  /// Create PicklerImpl for complex cases with delegation
  private static <T> PicklerImpl<T> createPicklerImpl(
      Class<T> rootClass,
      Set<Class<?>> recordClasses,
      Map<Class<?>, Set<Class<?>>> dependencies,
      Map<Class<?>, Long> typeSignatures) {

    // Create the shared resolver map that all instances will use
    final var serdes = new HashMap<Class<?>, Pickler<?>>();
    final var typeSignatureToSerde = new HashMap<Long, Pickler<?>>();

    // Compute type signatures for all record classes
    final var recordTypeSignatures = Companion.computeRecordTypeSignatures(recordClasses.stream().toList());

    // Create resolver callbacks that delegate to the shared map
    final SizerResolver sizerResolver = (targetClass) -> {
      final var serde = serdes.get(targetClass);
      if (serde == null) {
        throw new IllegalStateException("No serde found for class: " + targetClass);
      }
      return obj -> {
        @SuppressWarnings("unchecked") final var typedSerde = (Pickler<Object>) serde;
        return typedSerde.maxSizeOf(obj);
      };
    };

    final WriterResolver writerResolver = (targetClass) -> {
      final var serde = serdes.get(targetClass);
      if (serde == null) {
        throw new IllegalStateException("No serde found for class: " + targetClass);
      }
      return (buffer, obj) -> {
        @SuppressWarnings("unchecked") final var typedSerde = (Pickler<Object>) serde;
        typedSerde.serialize(buffer, obj);
      };
    };

    final ReaderResolver readerResolver = (typeSignature) -> {
      final var serde = typeSignatureToSerde.get(typeSignature);
      if (serde == null) {
        throw new IllegalStateException("No serde found for type signature: " + typeSignature);
      }
      return buffer -> {
        if (serde instanceof RecordSerde<?> recordSerde) {
          return recordSerde.deserializeWithoutSignature(buffer);
        } else if (serde instanceof EmptyRecordSerde<?> emptyRecordSerde) {
          return emptyRecordSerde.deserializeWithoutSignature(buffer);
        } else {
          throw new IllegalStateException("Unsupported serde type: " + serde.getClass());
        }
      };
    };

    // Create all RecordSerde instances with the shared resolvers
    for (Class<?> recordClass : recordClasses) {
      final var typeSignature = recordTypeSignatures.get(recordClass);
      final var altSignature = Optional.ofNullable(typeSignatures.get(recordClass));

      final Pickler<?> serde;
      if (recordClass.getRecordComponents().length == 0) {
        serde = new EmptyRecordSerde<>(recordClass, typeSignature, altSignature);
      } else {
        serde = new RecordSerde<>(
            recordClass,
            typeSignature,
            altSignature,
            sizerResolver,
            writerResolver,
            readerResolver
        );
      }

      serdes.put(recordClass, serde);
      typeSignatureToSerde.put(typeSignature, serde);
      if (altSignature.isPresent()) {
        typeSignatureToSerde.put(altSignature.get(), serde);
      }
    }

    return new PicklerImpl<>(rootClass, serdes, typeSignatureToSerde);
  }

  /// In order to support optional backwards compatibility, we need to be able to tell the newer pickler what is the
  /// type signature of the original class.
  long typeSignature(Class<?> originalClass);
}
