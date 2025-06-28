// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import java.lang.reflect.*;
import java.util.Objects;
import java.util.stream.Stream;

/// Public sealed interface for the Type Expression protocol
/// All type expression nodes are nested within this interface to provide a clean API
sealed interface TypeExpr permits
    TypeExpr.ArrayNode, TypeExpr.ListNode, TypeExpr.OptionalNode, TypeExpr.MapNode,
    TypeExpr.RefValueNode, TypeExpr.PrimitiveValueNode {


  static Stream<Class<?>> classesInAST(TypeExpr typeExpr) {
    return switch (typeExpr) {
      case TypeExpr.ArrayNode(var element) -> classesInAST(element);
      case TypeExpr.ListNode(var element) -> classesInAST(element);
      case TypeExpr.OptionalNode(var wrapped) -> classesInAST(wrapped);
      case TypeExpr.MapNode(var key, var value) -> Stream.concat(classesInAST(key), classesInAST(value));
      case TypeExpr.RefValueNode(var ignored, var type) -> switch (type) {
        case Class<?> clazz -> Stream.of(clazz);
        default -> Stream.empty();
      };
      case TypeExpr.PrimitiveValueNode(var ignored, var ignored2) -> Stream.empty();
    };
  }

  /// Recursive descent parser for Java types - builds tree bottom-up
  static TypeExpr analyzeType(Type type) {

    // Handle arrays first (both primitive arrays and object arrays)
    if (type instanceof Class<?> clazz) {
      if (clazz.isArray()) {
        TypeExpr elementTypeExpr = analyzeType(clazz.getComponentType());
        return new ArrayNode(elementTypeExpr);
      } else {
        if (clazz.isPrimitive()) {
          PrimitiveValueType primType = classifyPrimitiveClass(clazz);
          return new PrimitiveValueNode(primType, clazz);
        } else {
          RefValueType primType = classifyReferenceClass(clazz);
          return new RefValueNode(primType, clazz);
        }
      }
    }

    // Handle generic array types (e.g., T[] where T is a type parameter)
    if (type instanceof GenericArrayType genericArrayType) {
      TypeExpr elementTypeExpr = analyzeType(genericArrayType.getGenericComponentType());
      return new ArrayNode(elementTypeExpr);
    }

    // Handle parameterized types (List<T>, Map<K,V>, Optional<T>)
    if (type instanceof ParameterizedType paramType) {
      Type rawType = paramType.getRawType();

      if (rawType instanceof Class<?> rawClass) {
        Type[] typeArgs = paramType.getActualTypeArguments();

        // Handle List<T>
        if (java.util.List.class.isAssignableFrom(rawClass)) {
          if (typeArgs.length == 1) {
            TypeExpr elementTypeExpr = analyzeType(typeArgs[0]);
            return new ListNode(elementTypeExpr);
          } else {
            throw new IllegalArgumentException("List must have exactly one type argument: " + type);
          }
        }

        // Handle Optional<T>
        if (java.util.Optional.class.isAssignableFrom(rawClass)) {
          if (typeArgs.length == 1) {
            TypeExpr wrappedTypeExpr = analyzeType(typeArgs[0]);
            return new OptionalNode(wrappedTypeExpr);
          } else {
            throw new IllegalArgumentException("Optional must have exactly one type argument: " + type);
          }
        }

        // Handle Map<K,V>
        if (java.util.Map.class.isAssignableFrom(rawClass)) {
          if (typeArgs.length == 2) {
            TypeExpr keyTypeExpr = analyzeType(typeArgs[0]);
            TypeExpr valueTypeExpr = analyzeType(typeArgs[1]);
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

  /// Classifies a Java Class into the appropriate PrimitiveType
  static PrimitiveValueType classifyPrimitiveClass(Class<?> clazz) {
    // Handle primitive types and their boxed equivalents
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

    throw new IllegalArgumentException("Unsupported class type: " + clazz);
  }


  /// Classifies a Java Class into the appropriate PrimitiveType
  static RefValueType classifyReferenceClass(Class<?> clazz) {
    // Handle primitive types and their boxed equivalents
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

    // Handle reference types
    if (clazz == String.class) {
      return RefValueType.STRING;
    }
    if (clazz == java.util.UUID.class) {
      return RefValueType.UUID;
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

    throw new IllegalArgumentException("Unsupported class type: " + clazz);
  }

  /// Helper method to get a string representation for debugging
  /// Example: LIST(STRING) or MAP(STRING, INTEGER)
  default String toTreeString() {
    return switch (this) {
      case ArrayNode(var element) -> "ARRAY(" + element.toTreeString() + ")";
      case ListNode(var element) -> "LIST(" + element.toTreeString() + ")";
      case OptionalNode(var wrapped) -> "OPTIONAL(" + wrapped.toTreeString() + ")";
      case MapNode(var key, var value) -> "MAP(" + key.toTreeString() + ", " + value.toTreeString() + ")";
      case RefValueNode(var type, var ignored) -> type.name();
      case PrimitiveValueNode(var type, var ignored) -> type.name();
    };
  }

  /// Static method to analyze a type and return the TypeExpr
  /// This is the public API entry point that delegates to package-private implementation
  static TypeExpr analyze(Type type) {
    java.util.Objects.requireNonNull(type, "Type cannot be null");
    return analyzeType(type);
  }

  boolean isPrimitive();

  boolean isContainer();

  boolean isRecord();


  /// Container node for arrays - has one child (element type)
  record ArrayNode(TypeExpr element) implements TypeExpr {
    public ArrayNode {
      java.util.Objects.requireNonNull(element, "Array element type cannot be null");
    }

    @Override
    public boolean isPrimitive() {
      return false;
    }

    @Override
    public boolean isContainer() {
      return true;
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
    public boolean isContainer() {
      return true;
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
    public boolean isContainer() {
      return true;
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
    public boolean isContainer() {
      return true;
    }

    @Override
    public boolean isRecord() {
      return false;
    }
  }

  /// Leaf node for all primitive/value types
  /// Stores both the category (enum) and the actual Java type
  record RefValueNode(RefValueType type, Type javaType) implements TypeExpr {
    public RefValueNode {
      Objects.requireNonNull(type, "Primitive type cannot be null");
      Objects.requireNonNull(javaType, "Java type cannot be null");
    }

    /// Override to only show the type name, not the Java type
    @Override
    public String toTreeString() {
      return ((Class<?>) javaType()).getSimpleName();
    }

    @Override
    public boolean isPrimitive() {
      return false;
    }

    @Override
    public boolean isContainer() {
      return false;
    }

    @Override
    public boolean isRecord() {
      return this.type == RefValueType.RECORD;
    }
  }

  /// Enum for all value-like reference types (leaf nodes in the TypeExpr)
  enum RefValueType {
    BOOLEAN, BYTE, SHORT, CHARACTER,
    INTEGER, LONG, FLOAT, DOUBLE,
    STRING, UUID, ENUM, RECORD,
    INTERFACE;

  }

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
    public boolean isContainer() {
      return false;
    }

    @Override
    public boolean isRecord() {
      return false;
    }
  }

  /// Enum for all value-like reference types (leaf nodes in the TypeExpr)
  enum PrimitiveValueType {
    BOOLEAN, BYTE, SHORT, CHARACTER,
    INTEGER, LONG, FLOAT, DOUBLE
  }
}
