// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.jetbrains.annotations.NotNull;

import java.lang.reflect.*;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;

/// Public sealed interface for the Type Expression protocol with marker support
/// All type expression nodes are nested within this interface to provide a clean API
sealed interface TypeExpr permits
    TypeExpr.ArrayNode, TypeExpr.ListNode, TypeExpr.OptionalNode, TypeExpr.MapNode,
    TypeExpr.RefValueNode, TypeExpr.PrimitiveValueNode, TypeExpr.PrimitiveArrayNode {

  /// A special marker for user-defined types that are serialized using a type signature.
  /// This marker should never be written to the wire due to static analysis knowing we are dealing with a user type.
  int VOID_MARKER = 0;

  /// Recursive descent parser for Java types - builds tree bottom-up with markers
  static TypeExpr analyzeType(Type type, Collection<SerdeHandler> customHandlers) {
    final var result = analyzeTypeInner(type, customHandlers);
    LOGGER.finer(() -> "Got TypeExpr: " + result.toTreeString());
    return result;
  }

  private static @NotNull TypeExpr analyzeTypeInner(Type type, Collection<SerdeHandler> customHandlers) {
    LOGGER.finer(() -> "Analyzing type: " + type);

    // Handle arrays first (both primitive arrays and object arrays)
    if (type instanceof Class<?> clazz) {
      LOGGER.finer(() -> "Class type: " + clazz.getSimpleName());
      if (clazz.isArray()) {
        LOGGER.finer(() -> "Processing array type: " + clazz);
        final Class<?> componentType = clazz.getComponentType();
        if (componentType.isPrimitive()) {
          final var primitiveType = classifyPrimitiveClass(componentType);
          return new PrimitiveArrayNode(primitiveType, clazz);
        } else {
          TypeExpr elementTypeExpr = analyzeType(componentType, customHandlers);
          LOGGER.finer(() -> "Created array node for: " + clazz + " with element type: " + elementTypeExpr.toTreeString());
          return new ArrayNode(elementTypeExpr, componentType);
        }
      } else {
        if (clazz.isPrimitive()) {
          PrimitiveValueType primType = classifyPrimitiveClass(clazz);
          return new PrimitiveValueNode(primType, clazz);
        } else {
          // Check if it's a custom type first
          for (SerdeHandler handler : customHandlers) {
            if (handler.valueBasedLike().equals(clazz)) {
              return new RefValueNode(RefValueType.CUSTOM, clazz);
            }
          }
          // Otherwise it's a built-in reference type
          RefValueType refType = classifyReferenceClass(clazz);
          return new RefValueNode(refType, clazz);
        }
      }
    }

    // Handle generic array types (e.g., T[] where T is a type parameter)
    if (type instanceof GenericArrayType genericArrayType) {
      LOGGER.finer(() -> "Processing generic array type: " + genericArrayType);
      TypeExpr elementTypeExpr = analyzeType(genericArrayType.getGenericComponentType(), customHandlers);
      Type componentType = genericArrayType.getGenericComponentType();
      LOGGER.finer(() -> "Generic array component type: " + componentType + " of class " + componentType.getClass().getName());

      Class<?> rawComponentType = getRawClass(componentType);
      LOGGER.finer(() -> "Determined raw component type for generic array: " + rawComponentType.getName());
      return new ArrayNode(elementTypeExpr, rawComponentType);
    }

    // Handle parameterized types (List<T>, Map<K,V>, Optional<T>)
    if (type instanceof ParameterizedType paramType) {
      Type rawType = paramType.getRawType();

      if (rawType instanceof Class<?> rawClass) {
        Type[] typeArgs = paramType.getActualTypeArguments();

        // Handle List<T>
        if (java.util.List.class.isAssignableFrom(rawClass)) {
          if (typeArgs.length == 1) {
            TypeExpr elementTypeExpr = analyzeType(typeArgs[0], customHandlers);
            return new ListNode(elementTypeExpr);
          } else {
            throw new IllegalArgumentException("List must have exactly one type argument: " + type);
          }
        }

        // Handle Optional<T>
        if (java.util.Optional.class.isAssignableFrom(rawClass)) {
          if (typeArgs.length == 1) {
            TypeExpr wrappedTypeExpr = analyzeType(typeArgs[0], customHandlers);
            return new OptionalNode(wrappedTypeExpr);
          } else {
            throw new IllegalArgumentException("Optional must have exactly one type argument: " + type);
          }
        }

        // Handle Map<K,V>
        if (java.util.Map.class.isAssignableFrom(rawClass)) {
          if (typeArgs.length == 2) {
            TypeExpr keyTypeExpr = analyzeType(typeArgs[0], customHandlers);
            TypeExpr valueTypeExpr = analyzeType(typeArgs[1], customHandlers);
            return new MapNode(keyTypeExpr, valueTypeExpr);
          } else {
            throw new IllegalArgumentException("Map must have exactly two type arguments: " + type);
          }
        }
      }
    }

    // end of supported types
    if (type instanceof TypeVariable<?>) {
      throw new IllegalArgumentException("Type variables are not supported in serialization: " + type);
    }

    if (type instanceof WildcardType) {
      throw new IllegalArgumentException("Wildcard types are not supported in serialization: " + type);
    }

    throw new IllegalArgumentException("Unsupported type: " + type + " of class " + type.getClass());
  }

  private static Class<?> getRawClass(Type type) {
    if (type instanceof Class<?> cls) {
      return cls;
    }
    if (type instanceof ParameterizedType pt) {
      return (Class<?>) pt.getRawType();
    }
    if (type instanceof GenericArrayType gat) {
      Class<?> componentRawClass = getRawClass(gat.getGenericComponentType());
      // This creates an array class of the component's raw class.
      // e.g., if component is List<String>, this returns List[].class
      return java.lang.reflect.Array.newInstance(componentRawClass, 0).getClass();
    }
    throw new IllegalArgumentException("Cannot determine raw class for type: " + type);
  }

  /// Classifies a Java Class into the appropriate PrimitiveValueType
  static PrimitiveValueType classifyPrimitiveClass(Class<?> clazz) {
    // Handle primitive types
    if (clazz == boolean.class) {
      return PrimitiveValueType.BOOLEAN;
    }
    if (clazz == byte.class) {
      return PrimitiveValueType.BYTE;
    }
    if (clazz == short.class) {
      return PrimitiveValueType.SHORT;
    }
    if (clazz == char.class) {
      return PrimitiveValueType.CHARACTER;
    }
    if (clazz == int.class) {
      return PrimitiveValueType.INTEGER;
    }
    if (clazz == long.class) {
      return PrimitiveValueType.LONG;
    }
    if (clazz == float.class) {
      return PrimitiveValueType.FLOAT;
    }
    if (clazz == double.class) {
      return PrimitiveValueType.DOUBLE;
    }

    throw new IllegalArgumentException("Unsupported primitive class type: " + clazz);
  }

  /// Classifies a Java Class into the appropriate RefValueType (excluding custom types)
  static RefValueType classifyReferenceClass(Class<?> clazz) {
    // Handle boxed primitives
    if (clazz == Boolean.class) {
      return RefValueType.BOOLEAN;
    }
    if (clazz == Byte.class) {
      return RefValueType.BYTE;
    }
    if (clazz == Short.class) {
      return RefValueType.SHORT;
    }
    if (clazz == Character.class) {
      return RefValueType.CHARACTER;
    }
    if (clazz == Integer.class) {
      return RefValueType.INTEGER;
    }
    if (clazz == Long.class) {
      return RefValueType.LONG;
    }
    if (clazz == Float.class) {
      return RefValueType.FLOAT;
    }
    if (clazz == Double.class) {
      return RefValueType.DOUBLE;
    }

    // Handle built-in reference types
    if (clazz == String.class) {
      return RefValueType.STRING;
    }
    if (clazz == java.time.LocalDate.class) {
      return RefValueType.LOCAL_DATE;
    }
    if (clazz == java.time.LocalDateTime.class) {
      return RefValueType.LOCAL_DATE_TIME;
    }
    // FIXME WARNING WARNING UUID will be handled like String to get more tests fixed WARNING WARNING
    if (clazz == java.util.UUID.class) {
      return RefValueType.STRING; // UUID will be handled like String for now
    }

    // Handle user-defined types
    if (clazz.isEnum()) {
      return RefValueType.ENUM;
    }
    if (clazz.isRecord()) {
      return RefValueType.RECORD;
    }
    if (clazz.isInterface() || clazz.isSealed()) {
      return RefValueType.INTERFACE;
    }

    throw new IllegalArgumentException("Unsupported reference class type: " + clazz);
  }

  static Stream<Class<?>> classesInAST(TypeExpr structure) {
    return switch (structure) {
      case ArrayNode(var element, var javaType) -> Stream.concat(
          Stream.of(javaType),
          classesInAST(element));
      case PrimitiveArrayNode(var ignored1, var arrayType) -> Stream.of(arrayType);
      case ListNode(var element) -> classesInAST(element);
      case OptionalNode(var wrapped) -> classesInAST(wrapped);
      case MapNode(var key, var value) -> Stream.concat(
          classesInAST(key),
          classesInAST(value));
      case RefValueNode(var ignored, var javaType) -> Stream.of((Class<?>) javaType);
      case PrimitiveValueNode(var ignored2, var javaType) -> Stream.of((Class<?>) javaType);
    };
  }

  /// Helper method to get a string representation for debugging
  /// Example: LIST(STRING) or MAP(STRING, INTEGER)
  default String toTreeString() {
    return switch (this) {
      case ArrayNode(var element, var ignored) -> "ARRAY(" + element.toTreeString() + ")";
      case PrimitiveArrayNode(var ignored, var arrayType) ->
          "ARRAY(" + arrayType.getComponentType().getSimpleName() + ")";
      case ListNode(var element) -> "LIST(" + element.toTreeString() + ")";
      case OptionalNode(var wrapped) -> "OPTIONAL(" + wrapped.toTreeString() + ")";
      case MapNode(var key, var value) -> "MAP(" + key.toTreeString() + "," + value.toTreeString() + ")";
      case RefValueNode(var ignored, var javaType) -> ((Class<?>) javaType).getSimpleName();
      case PrimitiveValueNode(var ignored1, var javaType) -> ((Class<?>) javaType).getSimpleName();
    };
  }

  boolean isPrimitive();

  boolean isRecord();

  /// Container node for arrays - has one child (element type)
  record ArrayNode(TypeExpr element, Class<?> componentType) implements TypeExpr {
    public ArrayNode {
      java.util.Objects.requireNonNull(element, "Array element type cannot be null");
    }

    @Override
    public boolean isPrimitive() {
      return false;
    }

    @Override
    public boolean isRecord() {
      return false;
    }
  }

  /// Container node for primitive arrays
  record PrimitiveArrayNode(PrimitiveValueType primitiveType, Class<?> arrayType) implements TypeExpr {
    public PrimitiveArrayNode {
      java.util.Objects.requireNonNull(primitiveType, "Primitive type cannot be null");
      java.util.Objects.requireNonNull(arrayType, "Array type cannot be null");
      if (!arrayType.isArray() || !arrayType.getComponentType().isPrimitive()) {
        throw new IllegalArgumentException("PrimitiveArrayNode requires a primitive array type");
      }
    }

    @Override
    public boolean isPrimitive() {
      return false; // The container itself is not a primitive
    }

    @Override
    public boolean isRecord() {
      return false;
    }
  }

  /// Container node for lists - has one child (element type)
  record ListNode(TypeExpr element) implements TypeExpr {
    public ListNode {
      java.util.Objects.requireNonNull(element, "List element type cannot be null");
    }

    @Override
    public boolean isPrimitive() {
      return false;
    }

    @Override
    public boolean isRecord() {
      return false;
    }
  }

  /// Container node for optionals - has one child (wrapped type)
  record OptionalNode(TypeExpr wrapped) implements TypeExpr {
    public OptionalNode {
      java.util.Objects.requireNonNull(wrapped, "Optional wrapped type cannot be null");
    }

    @Override
    public boolean isPrimitive() {
      return false;
    }

    @Override
    public boolean isRecord() {
      return false;
    }
  }

  /// Container node for maps - has two children (key type, value type)
  record MapNode(TypeExpr key, TypeExpr value) implements TypeExpr {
    public MapNode {
      java.util.Objects.requireNonNull(key, "Map key type cannot be null");
      java.util.Objects.requireNonNull(value, "Map value type cannot be null");
    }

    @Override
    public boolean isPrimitive() {
      return false;
    }

    @Override
    public boolean isRecord() {
      return false;
    }
  }

  /// Leaf node for all reference types with marker
  record RefValueNode(RefValueType type, Type javaType) implements TypeExpr {
    public RefValueNode {
      Objects.requireNonNull(type, "Reference type cannot be null");
      Objects.requireNonNull(javaType, "Java type cannot be null");
    }

    /// Override to only show the type name, not the Java type
    @Override
    public String toTreeString() {
      return ((Class<?>) this.javaType()).getSimpleName();
    }

    @Override
    public boolean isPrimitive() {
      return false;
    }

    @Override
    public boolean isRecord() {
      return this.type == RefValueType.RECORD;
    }
  }

  /// Enum for all value-like reference types (leaf nodes in the TypeExpr)
  enum RefValueType {
    RECORD, INTERFACE, ENUM,
    BOOLEAN, BYTE, SHORT, CHARACTER,
    INTEGER, LONG, FLOAT, DOUBLE,
    STRING, LOCAL_DATE, LOCAL_DATE_TIME,
    CUSTOM // For user-defined value-based types like UUID

  }

  /// Leaf node for primitive types
  record PrimitiveValueNode(PrimitiveValueType type, Type javaType) implements TypeExpr {
    public PrimitiveValueNode {
      Objects.requireNonNull(type, "Primitive type cannot be null");
      Objects.requireNonNull(javaType, "Java type cannot be null");
    }

    @Override
    public String toTreeString() {
      return ((Class<?>) javaType()).getSimpleName();
    }

    @Override
    public boolean isPrimitive() {
      return true;
    }

    @Override
    public boolean isRecord() {
      return false;
    }
  }

  enum PrimitiveValueType {
    BOOLEAN, BYTE, SHORT, CHARACTER,
    INTEGER, LONG, FLOAT, DOUBLE
  }
}
