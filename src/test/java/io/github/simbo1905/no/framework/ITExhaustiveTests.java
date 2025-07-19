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
    return targetType.isOfType(TypeExpr.class);
  }

  @Override
  public @NotNull Set<Arbitrary<?>> provideFor(@NotNull TypeUsage targetType, @NotNull SubtypeProvider subtypeProvider) {
    return Collections.singleton(typeExprs());
  }

  @Provide
  Arbitrary<TypeExpr> typeExprs() {
    return generateStructuralPatterns();
  }

  private Class<?> toClass(TypeExpr typeExpr) {
    return switch (typeExpr) {
      case TypeExpr.PrimitiveValueNode(var ignored, var javaType) -> (Class<?>) javaType;
      case TypeExpr.RefValueNode(var ignored, var javaType) -> (Class<?>) javaType;
      case TypeExpr.ArrayNode(var element, var ignored) ->
        // Recursively determine the element class and create an array class from it.
          java.lang.reflect.Array.newInstance(toClass(element), 0).getClass();
      case TypeExpr.ListNode(var ignored) -> List.class;
      case TypeExpr.OptionalNode(var ignored) -> Optional.class;
      case TypeExpr.MapNode(var ignored, var ignored2) -> Map.class;
      case TypeExpr.PrimitiveArrayNode(var ignored, var arrayType) -> arrayType;
    };
  }

  private Arbitrary<TypeExpr> generateStructuralPatterns() {
    // Base value types - no nesting
    Arbitrary<TypeExpr> primitives = Arbitraries.of(
        new TypeExpr.PrimitiveValueNode(TypeExpr.PrimitiveValueType.INTEGER, int.class),
        new TypeExpr.PrimitiveValueNode(TypeExpr.PrimitiveValueType.BOOLEAN, boolean.class),
        new TypeExpr.PrimitiveValueNode(TypeExpr.PrimitiveValueType.DOUBLE, double.class),
        new TypeExpr.PrimitiveValueNode(TypeExpr.PrimitiveValueType.LONG, long.class),
        new TypeExpr.PrimitiveValueNode(TypeExpr.PrimitiveValueType.FLOAT, float.class),
        new TypeExpr.PrimitiveValueNode(TypeExpr.PrimitiveValueType.BYTE, byte.class),
        new TypeExpr.PrimitiveValueNode(TypeExpr.PrimitiveValueType.SHORT, short.class),
        new TypeExpr.PrimitiveValueNode(TypeExpr.PrimitiveValueType.CHARACTER, char.class)
    );

    Arbitrary<TypeExpr> boxedTypes = Arbitraries.of(
        new TypeExpr.RefValueNode(TypeExpr.RefValueType.INTEGER, Integer.class),
        new TypeExpr.RefValueNode(TypeExpr.RefValueType.BOOLEAN, Boolean.class),
        new TypeExpr.RefValueNode(TypeExpr.RefValueType.DOUBLE, Double.class),
        new TypeExpr.RefValueNode(TypeExpr.RefValueType.LONG, Long.class),
        new TypeExpr.RefValueNode(TypeExpr.RefValueType.FLOAT, Float.class),
        new TypeExpr.RefValueNode(TypeExpr.RefValueType.BYTE, Byte.class),
        new TypeExpr.RefValueNode(TypeExpr.RefValueType.SHORT, Short.class),
        new TypeExpr.RefValueNode(TypeExpr.RefValueType.CHARACTER, Character.class)
    );

    Arbitrary<TypeExpr> referenceTypes = Arbitraries.of(
        new TypeExpr.RefValueNode(TypeExpr.RefValueType.RECORD, TestRecord.class),
        new TypeExpr.RefValueNode(TypeExpr.RefValueType.ENUM, TestEnum.class),
        new TypeExpr.RefValueNode(TypeExpr.RefValueType.STRING, String.class),
        new TypeExpr.RefValueNode(TypeExpr.RefValueType.LOCAL_DATE, LocalDate.class),
        new TypeExpr.RefValueNode(TypeExpr.RefValueType.LOCAL_DATE_TIME, LocalDateTime.class)
    );

    Arbitrary<TypeExpr> valueTypes = Arbitraries.oneOf(primitives, boxedTypes, referenceTypes);

    return Arbitraries.oneOf(
        valueTypes,                           // Depth 0: Value types
        singleContainers(valueTypes),         // Depth 1: Container(Value)
        doubleContainers(valueTypes),
        tripleContainers(valueTypes)          // Depth 3: Essential combinations only
    );
  }

  private Arbitrary<TypeExpr> singleContainers(Arbitrary<TypeExpr> valueTypes) {
    final var stringType = new TypeExpr.RefValueNode(TypeExpr.RefValueType.STRING, String.class);
    return Arbitraries.oneOf(
        valueTypes.map(vt -> {
          if (vt instanceof TypeExpr.PrimitiveValueNode(
              TypeExpr.PrimitiveValueType type, java.lang.reflect.Type javaType
          )) {
            // Correctly create a PrimitiveArrayNode for primitive elements
            Class<?> componentType = (Class<?>) javaType;
            Class<?> arrayType = Array.newInstance(componentType, 0).getClass();
            return new TypeExpr.PrimitiveArrayNode(type, arrayType);
          } else {
            // Use ArrayNode for all other element types (references, containers)
            return new TypeExpr.ArrayNode(vt, toClass(vt));
          }
        }),
        valueTypes.map(TypeExpr.ListNode::new),
        valueTypes.map(TypeExpr.OptionalNode::new),
        // Map with String keys to avoid key type explosion
        valueTypes.map(v -> new TypeExpr.MapNode(stringType, v)),
        valueTypes.map(k -> new TypeExpr.MapNode(k, stringType))
    );
  }

  private Arbitrary<TypeExpr> doubleContainers(Arbitrary<TypeExpr> valueTypes) {
    Arbitrary<TypeExpr> singleContainers = singleContainers(valueTypes);
    final var stringType = new TypeExpr.RefValueNode(TypeExpr.RefValueType.STRING, String.class);

    return Arbitraries.oneOf(
        singleContainers.map(sc -> {
          if (sc instanceof TypeExpr.PrimitiveValueNode(
              TypeExpr.PrimitiveValueType type, java.lang.reflect.Type javaType
          )) {
            // Correctly create a PrimitiveArrayNode for primitive elements
            Class<?> componentType = (Class<?>) javaType;
            Class<?> arrayType = Array.newInstance(componentType, 0).getClass();
            return new TypeExpr.PrimitiveArrayNode(type, arrayType);
          } else {
            // Use ArrayNode for all other element types (references, containers)
            return new TypeExpr.ArrayNode(sc, toClass(sc));
          }
        }),      // Array(Container(Value))
        singleContainers.map(TypeExpr.ListNode::new),
        singleContainers.map(TypeExpr.OptionalNode::new),
        singleContainers.map(s -> new TypeExpr.MapNode(stringType, s)),
        singleContainers.map(s -> new TypeExpr.MapNode(s, stringType))
    );
  }

  private Arbitrary<TypeExpr> tripleContainers(Arbitrary<TypeExpr> valueTypes) {
    // Only essential patterns to avoid combinatorial explosion
    final var stringType = new TypeExpr.RefValueNode(TypeExpr.RefValueType.STRING, String.class);
    Arbitrary<TypeExpr> essentialDouble = Arbitraries.oneOf(
        valueTypes.map(v -> new TypeExpr.ArrayNode(new TypeExpr.ListNode(v), List.class)),           // Array(List(Value))
        valueTypes.map(v -> new TypeExpr.ListNode(
            (v instanceof TypeExpr.PrimitiveValueNode(
                TypeExpr.PrimitiveValueType type, java.lang.reflect.Type javaType
            )) ?
                new TypeExpr.PrimitiveArrayNode(type, Array.newInstance((Class<?>) javaType, 0).getClass()) :
                new TypeExpr.ArrayNode(v, toClass(v))
        )),           // List(Array(Value))
        valueTypes.map(v -> new TypeExpr.OptionalNode(
            (v instanceof TypeExpr.PrimitiveValueNode(
                TypeExpr.PrimitiveValueType type, java.lang.reflect.Type javaType
            )) ?
                new TypeExpr.PrimitiveArrayNode(type, Array.newInstance((Class<?>) javaType, 0).getClass()) :
                new TypeExpr.ArrayNode(v, toClass(v))
        )),       // Optional(Array(Value))
        valueTypes.map(v -> new TypeExpr.MapNode(stringType, new TypeExpr.ListNode(v))), // Map<String, List<Value>>
        valueTypes.map(v -> new TypeExpr.MapNode(new TypeExpr.ListNode(v), stringType))  // Map<List<Value>, String>
    );

    return essentialDouble.map(ed -> {
      if (ed instanceof TypeExpr.PrimitiveValueNode(
          TypeExpr.PrimitiveValueType type, java.lang.reflect.Type javaType
      )) {
        // Correctly create a PrimitiveArrayNode for primitive elements
        Class<?> componentType = (Class<?>) javaType;
        Class<?> arrayType = Array.newInstance(componentType, 0).getClass();
        return new TypeExpr.PrimitiveArrayNode(type, arrayType);
      } else {
        // Use ArrayNode for all other element types (references, containers)
        return new TypeExpr.ArrayNode(ed, toClass(ed));
      }
    });  // Array(essential double combinations)
  }

  private static final AtomicLong classCounter = new AtomicLong(0);

  @Property(generation = GenerationMode.EXHAUSTIVE)
  @SuppressWarnings("unchecked")
  void exhaustiveRoundTrip(@ForAll("typeExprs") TypeExpr typeExpr) throws Exception {
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
    try {
      assertDeepEquals(recordInstance, deserialized);
    } catch (AssertionError e) {
      LOGGER.severe(() -> """
          ==========================
          Assertion failed for type: %s
          --------------------------
          With source code:
          --------------------------
          %s
          --------------------------
          Original instance: %s
          --------------------------
          Deserialized instance: %s
          --------------------------
          """.formatted(typeExpr.toTreeString(), sourceCode, recordInstance, deserialized));
      throw e;
    }

  }

  private Arbitrary<Object> createInstanceArbitrary(TypeExpr typeExpr, Class<?> compiledClass) {
    Arbitrary<?> componentArbitrary = arbitraryFor(typeExpr);
    RecordComponent[] components = compiledClass.getRecordComponents();
    if (components.length != 1) {
      throw new IllegalStateException("Generated record should have exactly one component");
    }
    Class<?> componentType = components[0].getType();

    return componentArbitrary.map(value -> {
      try {
        return compiledClass.getConstructor(componentType).newInstance(value);
      } catch (Exception e) {
        throw new RuntimeException("Failed to instantiate generated record " + compiledClass.getName(), e);
      }
    });
  }

  private Arbitrary<?> arbitraryFor(TypeExpr typeExpr) {
    return switch (typeExpr) {
      case TypeExpr.PrimitiveValueNode(var type, var ignored) -> switch (type) {
        case BOOLEAN -> Arbitraries.of(true, false);
        case BYTE -> Arbitraries.bytes();
        case SHORT -> Arbitraries.shorts();
        case CHARACTER -> Arbitraries.chars();
        case INTEGER -> Arbitraries.integers();
        case LONG -> Arbitraries.longs();
        case FLOAT -> Arbitraries.floats();
        case DOUBLE -> Arbitraries.doubles();
      };
      case TypeExpr.RefValueNode(var type, var javaType) -> switch (type) {
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
      case TypeExpr.ArrayNode(var element, var ignoredComponentType) -> {
        // Recursively get an arbitrary for the array's elements.
        Arbitrary<?> elementArbitrary = arbitraryFor(element);

        // This is the class of the final array to be generated (e.g., Boolean[][].class).
        Class<?> arrayClass = toClass(typeExpr);

        // Manually create the array by first generating a list of elements
        // and then populating a new array instance. This is more robust
        // than using Arbitrary.array() in this recursive context.
        yield elementArbitrary.list().map(list -> {
          // The componentType is the type of the elements (e.g., Boolean[].class for a Boolean[][]).
          Class<?> componentType = arrayClass.getComponentType();
          Object array = Array.newInstance(componentType, list.size());
          for (int i = 0; i < list.size(); i++) {
            Array.set(array, i, list.get(i));
          }
          return array;
        });
      }
      case TypeExpr.ListNode(var element) -> arbitraryFor(element).list();
      case TypeExpr.OptionalNode(var wrapped) -> arbitraryFor(wrapped).optional();
      case TypeExpr.MapNode(var key, var value) -> Arbitraries.maps(arbitraryFor(key), arbitraryFor(value));
      // TODO: handle PrimitiveArrayNode
      case TypeExpr.PrimitiveArrayNode(var primitiveType, var arrayType) -> {
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
          Object primitiveArray = Array.newInstance(arrayType.getComponentType(), list.size());
          for (int i = 0; i < list.size(); i++) {
            Array.set(primitiveArray, i, list.get(i));
          }
          return primitiveArray;
        });
      }
    };
  }

  private String generateRecordSource(String recordName, TypeExpr typeExpr) {
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

  private void collectImports(TypeExpr typeExpr, Set<String> imports) {
    switch (typeExpr) {
      case TypeExpr.RefValueNode(var ignored, var javaType) -> {
        Class<?> clazz = (Class<?>) javaType;
        if (!clazz.isPrimitive() && !clazz.getPackageName().equals("java.lang")) {
          imports.add(clazz.getCanonicalName());
        }
      }
      case TypeExpr.ArrayNode(var element, var ignored) -> collectImports(element, imports);
      case TypeExpr.ListNode(var element) -> {
        imports.add("java.util.List");
        collectImports(element, imports);
      }
      case TypeExpr.OptionalNode(var wrapped) -> {
        imports.add("java.util.Optional");
        collectImports(wrapped, imports);
      }
      case TypeExpr.MapNode(var key, var value) -> {
        imports.add("java.util.Map");
        collectImports(key, imports);
        collectImports(value, imports);
      }
      case TypeExpr.PrimitiveValueNode(var ignored, var ignored2) -> {
      }
      case TypeExpr.PrimitiveArrayNode(var ignored, var ignored1) -> {
      }
    }
  }

  private String toJavaType(TypeExpr typeExpr, boolean inGeneric) {
    return switch (typeExpr) {
      case TypeExpr.PrimitiveValueNode(var type, var javaType) -> {
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
      case TypeExpr.RefValueNode(var ignored, var javaType) -> ((Class<?>) javaType).getCanonicalName();
      case TypeExpr.ArrayNode(var element, var ignored) -> toJavaType(element, false) + "[]";
      case TypeExpr.ListNode(var element) -> "java.util.List<" + toJavaType(element, true) + ">";
      case TypeExpr.OptionalNode(var wrapped) -> "java.util.Optional<" + toJavaType(wrapped, true) + ">";
      case TypeExpr.MapNode(var key, var value) ->
          "java.util.Map<" + toJavaType(key, true) + ", " + toJavaType(value, true) + ">";
      case TypeExpr.PrimitiveArrayNode(var ignored, var arrayType) -> arrayType.getCanonicalName();
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

      // Track which actual entries we've already matched
      boolean[] used = new boolean[actualMap.size()];
      List<Map.Entry<?, ?>> actualEntries = new ArrayList<>(actualMap.entrySet());

      outer:
      for (Map.Entry<?, ?> expectedEntry : expectedMap.entrySet()) {
        Object expectedKey = expectedEntry.getKey();
        Object expectedValue = expectedEntry.getValue();

        for (int i = 0; i < actualEntries.size(); i++) {
          if (used[i]) continue;
          Map.Entry<?, ?> actualEntry = actualEntries.get(i);
          try {
            assertDeepEquals(expectedKey, actualEntry.getKey());
            assertDeepEquals(expectedValue, actualEntry.getValue());
            used[i] = true; // Found a unique match, mark as used
            continue outer;
          } catch (AssertionError | Exception ignore) {
            // Not a match, keep looking
          }
        }
        throw new AssertionError("No matching entry in actual map for expected entry: " + expectedEntry);
      }
      // Optionally, check that all actual entries have been matched if duplication is possible
    } else if (expected instanceof Optional<?> expectedOptional && actual instanceof Optional<?> actualOptional) {
      assertEquals(expectedOptional.isPresent(), actualOptional.isPresent(), "Optional presence differs");
      if (expectedOptional.isPresent()) {
        //noinspection OptionalGetWithoutIsPresent
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
      TypeExpr componentTypeExpr = createTypeExprFromClass(componentType);
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

  private TypeExpr createTypeExprFromClass(Class<?> clazz) {
    if (clazz == int.class) {
      return new TypeExpr.PrimitiveValueNode(TypeExpr.PrimitiveValueType.INTEGER, int.class);
    }
    if (clazz == boolean.class) {
      return new TypeExpr.PrimitiveValueNode(TypeExpr.PrimitiveValueType.BOOLEAN, boolean.class);
    }
    if (clazz == Integer.class) {
      return new TypeExpr.RefValueNode(TypeExpr.RefValueType.INTEGER, Integer.class);
    }
    if (clazz == String.class) {
      return new TypeExpr.RefValueNode(TypeExpr.RefValueType.STRING, String.class);
    }
    if (clazz == TestRecord.class) {
      return new TypeExpr.RefValueNode(TypeExpr.RefValueType.RECORD, TestRecord.class);
    }
    if (clazz == TestEnum.class) {
      return new TypeExpr.RefValueNode(TypeExpr.RefValueType.ENUM, TestEnum.class);
    }
    // Add more mappings as needed
    throw new IllegalArgumentException("Unsupported class type: " + clazz);
  }
}
