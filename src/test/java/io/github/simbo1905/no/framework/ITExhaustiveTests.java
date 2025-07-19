// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import io.github.simbo1905.LoggingControl;
import net.jqwik.api.*;
import net.jqwik.api.providers.ArbitraryProvider;
import net.jqwik.api.providers.TypeUsage;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ITExhaustiveTests implements ArbitraryProvider {

  @BeforeAll
  static void setupLogging() {
    LoggingControl.setupCleanLogging();
  }

  

  @Override
  public boolean canProvideFor(TypeUsage targetType) {
    return targetType.isOfType(TypeExpr2.class);
  }

  @Override
  public @NotNull Set<Arbitrary<?>> provideFor(@NotNull TypeUsage targetType, @NotNull SubtypeProvider subtypeProvider) {
    return Collections.singleton(typeExprs());
  }

  @Provide
  Arbitrary<TypeExpr2> typeExprs() {
    return generateStructuralPatterns();
  }

  private Class<?> toClass(TypeExpr2 typeExpr) {
    return switch (typeExpr) {
      case TypeExpr2.PrimitiveValueNode(var ignored, var javaType) -> (Class<?>) javaType;
      case TypeExpr2.RefValueNode(var ignored, var javaType) -> (Class<?>) javaType;
      case TypeExpr2.ArrayNode(var element, var ignored) -> Array.newInstance(toClass(element), 0).getClass();
      case TypeExpr2.ListNode(var ignored) -> List.class;
      case TypeExpr2.OptionalNode(var ignored) -> Optional.class;
      case TypeExpr2.MapNode(var ignored, var ignored2) -> Map.class;
        case TypeExpr2.PrimitiveArrayNode(var ignored, var arrayType) -> (Class<?>) arrayType;
    };
  }

  private Arbitrary<TypeExpr2> generateStructuralPatterns() {
    // Base value types - no nesting
    Arbitrary<TypeExpr2> primitives = Arbitraries.of(
        new TypeExpr2.PrimitiveValueNode(TypeExpr2.PrimitiveValueType.INTEGER, int.class),
        new TypeExpr2.PrimitiveValueNode(TypeExpr2.PrimitiveValueType.BOOLEAN, boolean.class),
        new TypeExpr2.PrimitiveValueNode(TypeExpr2.PrimitiveValueType.DOUBLE, double.class),
        new TypeExpr2.PrimitiveValueNode(TypeExpr2.PrimitiveValueType.LONG, long.class),
        new TypeExpr2.PrimitiveValueNode(TypeExpr2.PrimitiveValueType.FLOAT, float.class),
        new TypeExpr2.PrimitiveValueNode(TypeExpr2.PrimitiveValueType.BYTE, byte.class),
        new TypeExpr2.PrimitiveValueNode(TypeExpr2.PrimitiveValueType.SHORT, short.class),
        new TypeExpr2.PrimitiveValueNode(TypeExpr2.PrimitiveValueType.CHARACTER, char.class)
    );

    Arbitrary<TypeExpr2> boxedTypes = Arbitraries.of(
        new TypeExpr2.RefValueNode(TypeExpr2.RefValueType.INTEGER, Integer.class),
        new TypeExpr2.RefValueNode(TypeExpr2.RefValueType.BOOLEAN, Boolean.class),
        new TypeExpr2.RefValueNode(TypeExpr2.RefValueType.DOUBLE, Double.class),
        new TypeExpr2.RefValueNode(TypeExpr2.RefValueType.LONG, Long.class),
        new TypeExpr2.RefValueNode(TypeExpr2.RefValueType.FLOAT, Float.class),
        new TypeExpr2.RefValueNode(TypeExpr2.RefValueType.BYTE, Byte.class),
        new TypeExpr2.RefValueNode(TypeExpr2.RefValueType.SHORT, Short.class),
        new TypeExpr2.RefValueNode(TypeExpr2.RefValueType.CHARACTER, Character.class)
    );

    Arbitrary<TypeExpr2> referenceTypes = Arbitraries.of(
        new TypeExpr2.RefValueNode(TypeExpr2.RefValueType.RECORD, TestRecord.class),
        new TypeExpr2.RefValueNode(TypeExpr2.RefValueType.ENUM, TestEnum.class),
        new TypeExpr2.RefValueNode(TypeExpr2.RefValueType.STRING, String.class),
        new TypeExpr2.RefValueNode(TypeExpr2.RefValueType.LOCAL_DATE, LocalDate.class),
        new TypeExpr2.RefValueNode(TypeExpr2.RefValueType.LOCAL_DATE_TIME, LocalDateTime.class)
    );

    Arbitrary<TypeExpr2> valueTypes = Arbitraries.oneOf(primitives, boxedTypes, referenceTypes);

    return Arbitraries.oneOf(
        valueTypes,                           // Depth 0: Value types
        singleContainers(valueTypes),         // Depth 1: Container(Value)
        doubleContainers(valueTypes),
        tripleContainers(valueTypes)          // Depth 3: Essential combinations only
    );
  }

  private Arbitrary<TypeExpr2> singleContainers(Arbitrary<TypeExpr2> valueTypes) {
    final var stringType = new TypeExpr2.RefValueNode(TypeExpr2.RefValueType.STRING, String.class);
    return Arbitraries.oneOf(
        valueTypes.map(vt -> new TypeExpr2.ArrayNode(vt, toClass(vt))),
        valueTypes.map(TypeExpr2.ListNode::new),
        valueTypes.map(TypeExpr2.OptionalNode::new),
        // Map with String keys to avoid key type explosion
        valueTypes.map(v -> new TypeExpr2.MapNode(stringType, v)),
        valueTypes.map(k -> new TypeExpr2.MapNode(k, stringType))
    );
  }

  private Arbitrary<TypeExpr2> doubleContainers(Arbitrary<TypeExpr2> valueTypes) {
    Arbitrary<TypeExpr2> singleContainers = singleContainers(valueTypes);
    final var stringType = new TypeExpr2.RefValueNode(TypeExpr2.RefValueType.STRING, String.class);

    return Arbitraries.oneOf(
        singleContainers.map(sc -> new TypeExpr2.ArrayNode(sc, toClass(sc))),      // Array(Container(Value))
        singleContainers.map(TypeExpr2.ListNode::new),
        singleContainers.map(TypeExpr2.OptionalNode::new),
        singleContainers.map(s -> new TypeExpr2.MapNode(stringType, s)),
        singleContainers.map(s -> new TypeExpr2.MapNode(s, stringType))
    );
  }

  private Arbitrary<TypeExpr2> tripleContainers(Arbitrary<TypeExpr2> valueTypes) {
    // Only essential patterns to avoid combinatorial explosion
    final var stringType = new TypeExpr2.RefValueNode(TypeExpr2.RefValueType.STRING, String.class);
    Arbitrary<TypeExpr2> essentialDouble = Arbitraries.oneOf(
        valueTypes.map(v -> new TypeExpr2.ArrayNode(new TypeExpr2.ListNode(v), List.class)),           // Array(List(Value))
        valueTypes.map(v -> new TypeExpr2.ListNode(new TypeExpr2.ArrayNode(v, toClass(v)))),           // List(Array(Value))
        valueTypes.map(v -> new TypeExpr2.OptionalNode(new TypeExpr2.ArrayNode(v, toClass(v)))),       // Optional(Array(Value))
        valueTypes.map(v -> new TypeExpr2.MapNode(stringType, new TypeExpr2.ListNode(v))), // Map<String, List<Value>>
        valueTypes.map(v -> new TypeExpr2.MapNode(new TypeExpr2.ListNode(v), stringType))  // Map<List<Value>, String>
    );

    return essentialDouble.map(ed -> new TypeExpr2.ArrayNode(ed, toClass(ed)));  // Array(essential double combinations)
  }

  private static final AtomicLong classCounter = new AtomicLong(0);

  @Property(generation = GenerationMode.EXHAUSTIVE)
  @SuppressWarnings("unchecked")
  void exhaustiveRoundTrip(@ForAll("typeExprs") TypeExpr2 typeExpr) throws Exception {
    String recordName = "GenRecord_" + classCounter.incrementAndGet();
    String fullClassName = "io.github.simbo1905.no.framework.generated." + recordName;

    String sourceCode = generateRecordSource(recordName, typeExpr);
    LOGGER.fine(() -> "Generated source for " + typeExpr.toTreeString() + ": " + sourceCode);

    Class<?> compiledClass = CompileAndLoadClass.compileAndClassLoad(fullClassName, sourceCode);

    Pickler<Object> pickler = Pickler.forClass((Class<Object>) compiledClass);

    Arbitrary<Object> instanceArbitrary = createInstanceArbitrary(typeExpr, compiledClass);

    Object recordInstance = instanceArbitrary.sample();

    ByteBuffer buffer = ByteBuffer.allocate(pickler.maxSizeOf(recordInstance));
    pickler.serialize(buffer, recordInstance);
    buffer.flip();
    Object deserialized = pickler.deserialize(buffer);

    assertDeepEquals(recordInstance, deserialized);
  }

  @SuppressWarnings("unchecked")
  private Arbitrary<Object> createInstanceArbitrary(TypeExpr2 typeExpr, Class<?> compiledClass) {
    Arbitrary<?> componentArbitrary = arbitraryFor(typeExpr);
    RecordComponent[] components = compiledClass.getRecordComponents();
    if (components.length != 1) {
      throw new IllegalStateException("Generated record should have exactly one component");
    }
    Class<?> componentType = components[0].getType();

    return (Arbitrary<Object>) componentArbitrary.map(value -> {
      try {
        return compiledClass.getConstructor(componentType).newInstance(value);
      } catch (Exception e) {
        throw new RuntimeException("Failed to instantiate generated record " + compiledClass.getName(), e);
      }
    });
  }

  @SuppressWarnings("unchecked")
  private Arbitrary<?> arbitraryFor(TypeExpr2 typeExpr) {
    return switch (typeExpr) {
      case TypeExpr2.PrimitiveValueNode(var type, var ignored) -> switch (type) {
        case BOOLEAN -> Arbitraries.of(true, false);
        case BYTE -> Arbitraries.bytes();
        case SHORT -> Arbitraries.shorts();
        case CHARACTER -> Arbitraries.chars();
        case INTEGER -> Arbitraries.integers();
        case LONG -> Arbitraries.longs();
        case FLOAT -> Arbitraries.floats();
        case DOUBLE -> Arbitraries.doubles();
      };
      case TypeExpr2.RefValueNode(var type, var javaType) -> switch (type) {
        case ENUM -> Arbitraries.defaultFor((Class<?>) javaType);
        case RECORD -> createRecordArbitrary((Class<?>) javaType);  // FIXED: Use custom method
        case BOOLEAN -> Arbitraries.of(true, false);
        case BYTE -> Arbitraries.bytes();
        case SHORT -> Arbitraries.shorts();
        case CHARACTER -> Arbitraries.chars();
        case INTEGER -> Arbitraries.integers();
        case LONG -> Arbitraries.longs();
        case FLOAT -> Arbitraries.floats();
        case DOUBLE -> Arbitraries.doubles();
        case STRING -> Arbitraries.strings();
        case LOCAL_DATE -> Arbitraries.defaultFor(LocalDate.class);
        case LOCAL_DATE_TIME -> Arbitraries.defaultFor(LocalDateTime.class);
        case INTERFACE -> Arbitraries.just(null);
        // TODO: handle CUSTOM type
        case CUSTOM -> Arbitraries.of();
      };
      case TypeExpr2.ArrayNode(var element, var componentType) -> {
          // JQwik's array() method needs the array class (int[].class), not the component type (int.class)
          Class<?> arrayClass = java.lang.reflect.Array.newInstance(componentType, 0).getClass();
          yield arbitraryFor(element).array((Class<Object>) arrayClass);
      }
      case TypeExpr2.ListNode(var element) -> arbitraryFor(element).list();
      case TypeExpr2.OptionalNode(var wrapped) -> arbitraryFor(wrapped).optional();
      case TypeExpr2.MapNode(var key, var value) -> Arbitraries.maps(arbitraryFor(key), arbitraryFor(value));
      // TODO: handle PrimitiveArrayNode
      case TypeExpr2.PrimitiveArrayNode(var primitiveType, var arrayType) -> {
        // JQwik does not directly support primitive arrays, so we create a list of wrappers and convert it.
        Arbitrary<?> wrapperArbitrary = switch (primitiveType) {
            case BOOLEAN -> Arbitraries.of(true, false);
            case BYTE -> Arbitraries.bytes();
            case SHORT -> Arbitraries.shorts();
            case CHARACTER -> Arbitraries.chars();
            case INTEGER -> Arbitraries.integers();
            case LONG -> Arbitraries.longs();
            case FLOAT -> Arbitraries.floats();
            case DOUBLE -> Arbitraries.doubles();
        };
        yield wrapperArbitrary.list().map(list -> {
            Object primitiveArray = Array.newInstance(((Class<?>) arrayType).getComponentType(), list.size());
            for (int i = 0; i < list.size(); i++) {
                Array.set(primitiveArray, i, list.get(i));
            }
            return primitiveArray;
        });
      }
    };
  }

  private String generateRecordSource(String recordName, TypeExpr2 typeExpr) {
    String typeName = toJavaType(typeExpr, false);
    String packageName = "io.github.simbo1905.no.framework.generated";

    // Collect all necessary imports
    Set<String> imports = new HashSet<>();
    collectImports(typeExpr, imports);

    StringBuilder importStatements = new StringBuilder();
    for (String imp : imports) {
      importStatements.append("import ").append(imp).append("; ");
    }

    String template = """
        package %s;
        
        %s
        import io.github.simbo1905.no.framework.TestEnum;
        import io.github.simbo1905.no.framework.TestRecord;
        
        public record %s(%s value) {}
        """;
    return String.format(template, packageName, importStatements, recordName, typeName);
  }

  private void collectImports(TypeExpr2 typeExpr, Set<String> imports) {
    switch (typeExpr) {
      case TypeExpr2.RefValueNode(var ignored, var javaType) -> {
        Class<?> clazz = (Class<?>) javaType;
        if (!clazz.isPrimitive() && !clazz.getPackageName().equals("java.lang")) {
          imports.add(clazz.getCanonicalName());
        }
      }
      case TypeExpr2.ArrayNode(var element, var ignored) -> collectImports(element, imports);
      case TypeExpr2.ListNode(var element) -> {
        imports.add("java.util.List");
        collectImports(element, imports);
      }
      case TypeExpr2.OptionalNode(var wrapped) -> {
        imports.add("java.util.Optional");
        collectImports(wrapped, imports);
      }
      case TypeExpr2.MapNode(var key, var value) -> {
        imports.add("java.util.Map");
        collectImports(key, imports);
        collectImports(value, imports);
      }
      case TypeExpr2.PrimitiveValueNode(var ignored, var ignored2) -> {
      }
      case TypeExpr2.PrimitiveArrayNode(var primitiveType, var arrayType) -> {
      }
    }
  }

  private String toJavaType(TypeExpr2 typeExpr, boolean inGeneric) {
    return switch (typeExpr) {
      case TypeExpr2.PrimitiveValueNode(var type, var javaType) -> {
        if (inGeneric) {
          yield switch (type) {
            case INTEGER -> "Integer";
            case BOOLEAN -> "Boolean";
            case DOUBLE -> "Double";
            case LONG -> "Long";
            case FLOAT -> "Float";
            case BYTE -> "Byte";
            case SHORT -> "Short";
            case CHARACTER -> "Character";
          };
        }
        yield ((Class<?>) javaType).getName();
      }
      case TypeExpr2.RefValueNode(var ignored, var javaType) -> ((Class<?>) javaType).getCanonicalName();
      case TypeExpr2.ArrayNode(var element, var ignored) -> toJavaType(element, false) + "[]";
      case TypeExpr2.ListNode(var element) -> "java.util.List<" + toJavaType(element, true) + ">";
      case TypeExpr2.OptionalNode(var wrapped) -> "java.util.Optional<" + toJavaType(wrapped, true) + ">";
      case TypeExpr2.MapNode(var key, var value) ->
          "java.util.Map<" + toJavaType(key, true) + ", " + toJavaType(value, true) + ">";
      case TypeExpr2.PrimitiveArrayNode(var primitiveType, var arrayType) -> ((Class<?>) arrayType).getCanonicalName();
    };
  }

  void assertDeepEquals(Object expected, Object actual) throws Exception {
    if (expected == null || actual == null) {
      assertEquals(expected, actual);
      return;
    }

    Class<?> expectedClass = expected.getClass();
    if (expectedClass.isRecord()) {
      for (RecordComponent component : expectedClass.getRecordComponents()) {
        Method accessor = component.getAccessor();
        Object expectedValue = accessor.invoke(expected);
        Object actualValue = accessor.invoke(actual);
        assertDeepEquals(expectedValue, actualValue);
      }
    } else if (expectedClass.isArray()) {
      assertDeepArrayEquals(expected, actual);
    } else if (expected instanceof List<?> expectedList && actual instanceof List<?> actualList) {
      assertEquals(expectedList.size(), actualList.size(), "List size differs");
      for (int i = 0; i < expectedList.size(); i++) {
        assertDeepEquals(expectedList.get(i), actualList.get(i));
      }
    } else if (expected instanceof Map<?, ?> expectedMap && actual instanceof Map<?, ?> actualMap) {
      assertEquals(expectedMap.size(), actualMap.size(), "Map size differs");
      for (Map.Entry<?, ?> entry : expectedMap.entrySet()) {
        Object expectedKey = entry.getKey();
        Object expectedValue = entry.getValue();
        // This is tricky for maps with non-trivial key equality
        Optional<?> actualKeyOpt = actualMap.keySet().stream()
            .filter(k -> {
              try {
                assertDeepEquals(expectedKey, k);
                return true;
              } catch (Exception | Error e) {
                return false;
              }
            })
            .findFirst();
        if (actualKeyOpt.isEmpty()) {
          throw new AssertionError("No matching key for " + expectedKey + " in actual map");
        }
        assertDeepEquals(expectedValue, actualMap.get(actualKeyOpt.get()));
      }
    } else if (expected instanceof Optional<?> expectedOptional && actual instanceof Optional<?> actualOptional) {
      assertEquals(expectedOptional.isPresent(), actualOptional.isPresent(), "Optional presence differs");
      if (expectedOptional.isPresent()) {
        assertDeepEquals(expectedOptional.get(), actualOptional.get());
      }
    } else {
      assertEquals(expected, actual);
    }
  }

  void assertDeepArrayEquals(Object expected, Object actual) throws Exception {
    if (expected == null || actual == null) {
      assertEquals(expected, actual, "One array is null while the other is not");
      return;
    }

    if (!expected.getClass().isArray() || !actual.getClass().isArray()) {
      assertEquals(expected, actual, "Expected array types but got different types");
      return;
    }

    int expectedLength = Array.getLength(expected);
    int actualLength = Array.getLength(actual);
    assertEquals(expectedLength, actualLength, "Array lengths differ");

    for (int i = 0; i < expectedLength; i++) {
      Object expectedElement = Array.get(expected, i);
      Object actualElement = Array.get(actual, i);
      assertDeepEquals(expectedElement, actualElement);
    }
  }

 @SuppressWarnings("unchecked")
private Arbitrary<?> createRecordArbitrary(Class<?> recordClass) {
    if (recordClass == TestRecord.class) {
        // Create arbitrary for TestRecord(int value)
        return Arbitraries.integers().map(TestRecord::new);
    }
    
    // For other record types, try to use reflection to build them
    RecordComponent[] components = recordClass.getRecordComponents();
    if (components.length == 0) {
        // No-arg record constructor
        try {
            java.lang.reflect.Constructor<?> constructor = recordClass.getConstructor();
            return Arbitraries.just(constructor.newInstance());
        } catch (Exception e) {
            throw new IllegalStateException("Cannot create arbitrary for record: " + recordClass, e);
        }
    }
    
    if (components.length == 1) {
        // Single component record
        Class<?> componentType = components[0].getType();
        TypeExpr2 componentTypeExpr = createTypeExprFromClass(componentType);
        Arbitrary<?> componentArbitrary = arbitraryFor(componentTypeExpr);
        
        return componentArbitrary.map(value -> {
            try {
                java.lang.reflect.Constructor<?> constructor = recordClass.getConstructor(componentType);
                return constructor.newInstance(value);
            } catch (Exception e) {
                throw new RuntimeException("Failed to create record instance", e);
            }
        });
    }
    
    // For multi-component records, this would need more complex handling
    throw new IllegalStateException("Multi-component record handling not implemented: " + recordClass);
}

private TypeExpr2 createTypeExprFromClass(Class<?> clazz) {
    if (clazz == int.class) {
        return new TypeExpr2.PrimitiveValueNode(TypeExpr2.PrimitiveValueType.INTEGER, int.class);
    }
    if (clazz == boolean.class) {
        return new TypeExpr2.PrimitiveValueNode(TypeExpr2.PrimitiveValueType.BOOLEAN, boolean.class);
    }
    if (clazz == Integer.class) {
        return new TypeExpr2.RefValueNode(TypeExpr2.RefValueType.INTEGER, Integer.class);
    }
    if (clazz == String.class) {
        return new TypeExpr2.RefValueNode(TypeExpr2.RefValueType.STRING, String.class);
    }
    if (clazz == TestRecord.class) {
        return new TypeExpr2.RefValueNode(TypeExpr2.RefValueType.RECORD, TestRecord.class);
    }
    if (clazz == TestEnum.class) {
        return new TypeExpr2.RefValueNode(TypeExpr2.RefValueType.ENUM, TestEnum.class);
    }
    // Add more mappings as needed
    throw new IllegalArgumentException("Unsupported class type: " + clazz);
}
}
