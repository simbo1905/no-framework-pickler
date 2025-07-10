// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.jetbrains.annotations.NotNull;

import java.lang.reflect.*;
import java.util.Collection;
import java.util.Objects;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;

/// Public sealed interface for the Type Expression protocol with marker support
/// All type expression nodes are nested within this interface to provide a clean API
sealed interface TypeExpr2 permits
    TypeExpr2.ArrayNode, TypeExpr2.ListNode, TypeExpr2.OptionalNode, TypeExpr2.MapNode,
    TypeExpr2.RefValueNode, TypeExpr2.PrimitiveValueNode {

  /// Get marker for a primitive type
  static int primitiveToMarker(Class<?> primitiveClass) {
    return switch (primitiveClass) {
      case Class<?> c when c == boolean.class -> -2;
      case Class<?> c when c == byte.class -> -3;
      case Class<?> c when c == short.class -> -4;
      case Class<?> c when c == char.class -> -5;
      case Class<?> c when c == int.class -> -6;
      case Class<?> c when c == long.class -> -8;
      case Class<?> c when c == float.class -> -10;
      case Class<?> c when c == double.class -> -11;
      default -> throw new IllegalArgumentException("Not a primitive type: " + primitiveClass);
    };
  }

  /// Get primitive type from marker
  static Class<?> markerToPrimitive(int marker) {
    return switch (marker) {
      case -2 -> boolean.class;
      case -3 -> byte.class;
      case -4 -> short.class;
      case -5 -> char.class;
      case -6, -7 -> int.class; // INTEGER and INTEGER_VAR
      case -8, -9 -> long.class; // LONG and LONG_VAR
      case -10 -> float.class;
      case -11 -> double.class;
      default -> throw new IllegalArgumentException("Not a primitive marker: " + marker);
    };
  }

  /// Get marker for built-in reference types
  static int referenceToMarker(Class<?> refClass) {
    return switch (refClass) {
      case Class<?> c when c == Boolean.class -> -2;
      case Class<?> c when c == Byte.class -> -3;
      case Class<?> c when c == Short.class -> -4;
      case Class<?> c when c == Character.class -> -5;
      case Class<?> c when c == Integer.class -> -6;
      case Class<?> c when c == Long.class -> -8;
      case Class<?> c when c == Float.class -> -10;
      case Class<?> c when c == Double.class -> -11;
      case Class<?> c when c == String.class -> -12;
      default -> throw new IllegalArgumentException("Not a built-in reference type: " + refClass);
    };
  }

  /// Container type markers
  static int containerToMarker(String containerType) {
    return switch (containerType) {
      case "OPTIONAL_EMPTY" -> -16;
      case "OPTIONAL_OF" -> -17;
      case "ARRAY" -> -18;
      case "MAP" -> -19;
      case "LIST" -> -20;
      default -> throw new IllegalArgumentException("Not a container type: " + containerType);
    };
  }

  /// Array element type to specific array marker
  static int arrayElementToMarker(TypeExpr2 element) {
    return switch (element) {
      case RefValueNode(RefValueType refType, var ignored1, var ignored2) -> switch (refType) {
        case RECORD -> -21;
        case INTERFACE -> -22;
        case ENUM -> -23;
        case BOOLEAN -> -24;
        case BYTE -> -25;
        case SHORT -> -26;
        case CHARACTER -> -27;
        case INTEGER -> -28;
        case LONG -> -29;
        case FLOAT -> -30;
        case DOUBLE -> -31;
        case STRING -> -32;
        case LOCAL_DATE -> -34;
        case LOCAL_DATE_TIME -> -35;
        case CUSTOM -> throw new IllegalArgumentException("Array of custom types not yet supported");
      };
      case ArrayNode(var ignored) -> -36;
      case ListNode(var ignored) -> -37;
      case MapNode ignored -> -38;
      case OptionalNode ignored -> -39;
      case PrimitiveValueNode ignored -> throw new IllegalArgumentException("Primitive arrays have dedicated markers");
    };
  }

  /// Recursive descent parser for Java types - builds tree bottom-up with markers
  static TypeExpr2 analyzeType(Type type, Collection<SerdeHandler> customHandlers) {
    final var result = analyzeTypeInner(type, customHandlers);
    LOGGER.finer(() -> "Got TypeExpr2: " + result.toTreeString());
    return result;
  }

  private static @NotNull TypeExpr2 analyzeTypeInner(Type type, Collection<SerdeHandler> customHandlers) {
    LOGGER.finer(() -> "Analyzing type: " + type);

    // Handle arrays first (both primitive arrays and object arrays)
    if (type instanceof Class<?> clazz) {
      LOGGER.finer(() -> "Class type: " + clazz.getSimpleName());
      if (clazz.isArray()) {
        LOGGER.finer(() -> "Processing array type: " + clazz);
        TypeExpr2 elementTypeExpr = analyzeType(clazz.getComponentType(), customHandlers);
        LOGGER.finer(() -> "Created array node for: " + clazz + " with element type: " + elementTypeExpr.toTreeString());
        return new ArrayNode(elementTypeExpr);
      } else {
        if (clazz.isPrimitive()) {
          PrimitiveValueType primType = classifyPrimitiveClass(clazz);
          int marker = primitiveToMarker(clazz);
          return new PrimitiveValueNode(primType, clazz, marker);
        } else {
          // Check if it's a custom type first
          for (SerdeHandler handler : customHandlers) {
            if (handler.valueBasedLike().equals(clazz)) {
              return new RefValueNode(RefValueType.CUSTOM, clazz, handler.marker());
            }
          }
          // Otherwise it's a built-in reference type
          RefValueType refType = classifyReferenceClass(clazz);
          int marker;
          if (refType == RefValueType.RECORD || refType == RefValueType.ENUM || refType == RefValueType.INTERFACE) {
            marker = 0; // User types use type signature
          } else {
            marker = referenceToMarker(clazz);
          }
          return new RefValueNode(refType, clazz, marker);
        }
      }
    }

    // Handle generic array types (e.g., T[] where T is a type parameter)
    if (type instanceof GenericArrayType genericArrayType) {
      TypeExpr2 elementTypeExpr = analyzeType(genericArrayType.getGenericComponentType(), customHandlers);
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
            TypeExpr2 elementTypeExpr = analyzeType(typeArgs[0], customHandlers);
            return new ListNode(elementTypeExpr);
          } else {
            throw new IllegalArgumentException("List must have exactly one type argument: " + type);
          }
        }

        // Handle Optional<T>
        if (java.util.Optional.class.isAssignableFrom(rawClass)) {
          if (typeArgs.length == 1) {
            TypeExpr2 wrappedTypeExpr = analyzeType(typeArgs[0], customHandlers);
            return new OptionalNode(wrappedTypeExpr);
          } else {
            throw new IllegalArgumentException("Optional must have exactly one type argument: " + type);
          }
        }

        // Handle Map<K,V>
        if (java.util.Map.class.isAssignableFrom(rawClass)) {
          if (typeArgs.length == 2) {
            TypeExpr2 keyTypeExpr = analyzeType(typeArgs[0], customHandlers);
            TypeExpr2 valueTypeExpr = analyzeType(typeArgs[1], customHandlers);
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

  /// Helper method to get a string representation for debugging
  /// Example: LIST(STRING) or MAP(STRING, INTEGER)
  default String toTreeString() {
    return switch (this) {
      case ArrayNode(var element) -> "ARRAY(" + element.toTreeString() + ")";
      case ListNode(var element) -> "LIST(" + element.toTreeString() + ")";
      case OptionalNode(var wrapped) -> "OPTIONAL(" + wrapped.toTreeString() + ")";
      case MapNode(var key, var value) -> "MAP(" + key.toTreeString() + "," + value.toTreeString() + ")";
      case RefValueNode(var type, var javaType, var marker) -> {
        String className = ((Class<?>) javaType).getSimpleName();
        if (type == RefValueType.CUSTOM) {
          yield className + "[m=" + marker + "]";
        } else if (marker == 0) {
          yield className + "[sig]";
        } else {
          yield className;
        }
      }
      case PrimitiveValueNode(var ignored1, var javaType, var ignored2) -> ((Class<?>) javaType).getSimpleName();
    };
  }

  /// Get the marker for this type expression
  int marker();

  boolean isPrimitive();

  boolean isRecord();

  /// Container node for arrays - has one child (element type)
  record ArrayNode(TypeExpr2 element) implements TypeExpr2 {
    public ArrayNode {
      java.util.Objects.requireNonNull(element, "Array element type cannot be null");
    }

    @Override
    public int marker() {
      // Array has its own marker
      return containerToMarker("ARRAY");
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

  /// Container node for lists - has one child (element type)
  record ListNode(TypeExpr2 element) implements TypeExpr2 {
    public ListNode {
      java.util.Objects.requireNonNull(element, "List element type cannot be null");
    }

    @Override
    public int marker() {
      return containerToMarker("LIST");
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
  record OptionalNode(TypeExpr2 wrapped) implements TypeExpr2 {
    public OptionalNode {
      java.util.Objects.requireNonNull(wrapped, "Optional wrapped type cannot be null");
    }

    @Override
    public int marker() {
      // Optional has two markers: EMPTY and OF
      // This is the container marker, actual wire format uses OPTIONAL_EMPTY/OPTIONAL_OF
      return containerToMarker("OPTIONAL_OF");
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
  record MapNode(TypeExpr2 key, TypeExpr2 value) implements TypeExpr2 {
    public MapNode {
      java.util.Objects.requireNonNull(key, "Map key type cannot be null");
      java.util.Objects.requireNonNull(value, "Map value type cannot be null");
    }

    @Override
    public int marker() {
      return containerToMarker("MAP");
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
  record RefValueNode(RefValueType type, Type javaType, int marker) implements TypeExpr2 {
    public RefValueNode {
      Objects.requireNonNull(type, "Reference type cannot be null");
      Objects.requireNonNull(javaType, "Java type cannot be null");
    }

    /// Override to only show the type name, not the Java type
    @Override
    public String toTreeString() {
      String className = ((Class<?>) javaType()).getSimpleName();
      if (type == RefValueType.CUSTOM) {
        return className + "[m=" + marker + "]";
      } else if (marker == 0) {
        return className + "[sig]";
      } else {
        return className;
      }
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

  /// Leaf node for primitive types with marker
  record PrimitiveValueNode(PrimitiveValueType type, Type javaType, int marker) implements TypeExpr2 {
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

  /// Enum for all primitive types
  enum PrimitiveValueType {
    BOOLEAN, BYTE, SHORT, CHARACTER,
    INTEGER, LONG, FLOAT, DOUBLE

  }
}
