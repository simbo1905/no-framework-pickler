package io.github.simbo1905.no.framework;

import net.jqwik.api.*;
import net.jqwik.api.providers.ArbitraryProvider;
import net.jqwik.api.providers.TypeUsage;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExhaustiveTest implements ArbitraryProvider {

  private static final Logger log = Logger.getLogger(ExhaustiveTest.class.getName());

  static {
    // To see the generated source code in the console
    log.setLevel(Level.INFO);
  }

  // Add these test are just to have things that can be nested
  public enum TestEnum {A}

  public record TestRecord(int value) {
  }


  @Override
  public boolean canProvideFor(TypeUsage targetType) {
    return targetType.isOfType(TypeExpr.class);
  }

  @Override
  public Set<Arbitrary<?>> provideFor(TypeUsage targetType, SubtypeProvider subtypeProvider) {
    return Collections.singleton(typeExprs());
  }

  @Provide
  Arbitrary<TypeExpr> typeExprs() {
    return generateStructuralPatterns();
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
        new TypeExpr.RefValueNode(TypeExpr.RefValueType.STRING, String.class),
        new TypeExpr.RefValueNode(TypeExpr.RefValueType.UUID, UUID.class),
        new TypeExpr.RefValueNode(TypeExpr.RefValueType.RECORD, TestRecord.class),
        new TypeExpr.RefValueNode(TypeExpr.RefValueType.ENUM, TestEnum.class)
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
    return Arbitraries.oneOf(
        valueTypes.map(TypeExpr.ArrayNode::new),
        valueTypes.map(TypeExpr.ListNode::new),
        valueTypes.map(TypeExpr.OptionalNode::new),
        // Map with String keys to avoid key type explosion
        valueTypes.map(v -> new TypeExpr.MapNode(new TypeExpr.RefValueNode(TypeExpr.RefValueType.STRING, String.class), v))
    );
  }

  private Arbitrary<TypeExpr> doubleContainers(Arbitrary<TypeExpr> valueTypes) {
    Arbitrary<TypeExpr> singleContainers = singleContainers(valueTypes);

    return Arbitraries.oneOf(
        singleContainers.map(TypeExpr.ArrayNode::new),      // Array(Container(Value))
        singleContainers.map(TypeExpr.ListNode::new),
        singleContainers.map(TypeExpr.OptionalNode::new),
        singleContainers.map(s -> new TypeExpr.MapNode(new TypeExpr.RefValueNode(TypeExpr.RefValueType.STRING, String.class), s))
    );
  }

  private Arbitrary<TypeExpr> tripleContainers(Arbitrary<TypeExpr> valueTypes) {
    // Only essential patterns to avoid combinatorial explosion
    Arbitrary<TypeExpr> essentialDouble = Arbitraries.oneOf(
        valueTypes.map(v -> new TypeExpr.ArrayNode(new TypeExpr.ListNode(v))),           // Array(List(Value))
        valueTypes.map(v -> new TypeExpr.ListNode(new TypeExpr.ArrayNode(v))),           // List(Array(Value))
        valueTypes.map(v -> new TypeExpr.OptionalNode(new TypeExpr.ArrayNode(v))),       // Optional(Array(Value))
        valueTypes.map(v -> new TypeExpr.ListNode(new TypeExpr.OptionalNode(v)))
    );

    return essentialDouble.map(TypeExpr.ArrayNode::new);  // Array(essential double combinations)
  }

  @Property(generation = GenerationMode.EXHAUSTIVE)
  @SuppressWarnings("unchecked")
  void exhaustiveGenerateAllClassesAsSrcWithRecordClassAndInstanceThenRoundTrip(@ForAll("typeExprs") TypeExpr typeExpr) throws Exception {
    String recordName = "GeneratedRecord";
    String fullClassName = "io.github.simbo1905.no.framework.generated." + recordName;

    String sourceCode = generateRecordSource(recordName, typeExpr);
    log.fine(() -> "Generated source for " + typeExpr.toTreeString() + ":\n" + sourceCode);

    Class<?> compiledClass = RecordSourceCodeToClassLoadWithInstance.compileAndClassLoad(fullClassName, sourceCode);
    TestableRecord instance = (TestableRecord) compiledClass.getConstructor().newInstance();
    Object recordInstance = instance.instance();

    Pickler<Object> pickler = Pickler.forClass((Class<Object>) recordInstance.getClass());

    ByteBuffer buffer = ByteBuffer.allocate(pickler.maxSizeOf(recordInstance));
    pickler.serialize(buffer, recordInstance);
    buffer.flip();
    Object deserialized = pickler.deserialize(buffer);

    // Custom deep equals for arrays
    assertDeepEquals(recordInstance, deserialized);
  }

  private String generateRecordSource(String recordName, TypeExpr typeExpr) {
    String typeName = toJavaType(typeExpr);
    String instanceValue = generateInstanceValue(typeExpr);
    String defaultValue = generateDefaultValue(typeExpr);

    return String.format("""
        package io.github.simbo1905.no.framework.generated;
        import static io.github.simbo1905.no.framework.ExhaustiveTest.*;
        
        import java.util.*;
        import io.github.simbo1905.no.framework.ExhaustiveTest.TestEnum;
        import io.github.simbo1905.no.framework.ExhaustiveTest.TestRecord;
        import io.github.simbo1905.no.framework.TestableRecord;
        
        public record %s(%s value) implements TestableRecord {
            public %s() {
                this(%s);
            }
        
            public Object instance() {
                return new %s(%s);
            }
        }
        """, recordName, typeName, recordName, defaultValue, recordName, instanceValue);
  }

  private String generateDefaultValue(TypeExpr typeExpr) {
    if (typeExpr instanceof TypeExpr.PrimitiveValueNode p) {
      return switch (p.type()) {
        case BOOLEAN -> "false";
        case BYTE -> "(byte) 0";
        case SHORT -> "(short) 0";
        case CHARACTER -> "'\u0000'";
        case INTEGER -> "0";
        case LONG -> "0L";
        case FLOAT -> "0.0f";
        case DOUBLE -> "0.0d";
      };
    }
    return "null";
  }

  private String toJavaType(TypeExpr typeExpr) {
    return toJavaType(typeExpr, false);
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
      case TypeExpr.RefValueNode(var type, var javaType) -> {
        Class<?> clazz = (Class<?>) javaType;
        if (clazz.equals(TestRecord.class) || clazz.equals(TestEnum.class)) {
          yield clazz.getCanonicalName();
        }
        yield clazz.getSimpleName();
      }
      case TypeExpr.ArrayNode(var element) -> toJavaType(element, true) + "[]";
      case TypeExpr.ListNode(var element) -> "List<" + toJavaType(element, true) + ">";
      case TypeExpr.OptionalNode(var wrapped) -> "Optional<" + toJavaType(wrapped, true) + ">";
      case TypeExpr.MapNode(var key, var value) ->
          "Map<" + toJavaType(key, true) + ", " + toJavaType(value, true) + ">";
    };
  }

  private String generateInstanceValue(TypeExpr typeExpr) {
    return switch (typeExpr) {
      case TypeExpr.PrimitiveValueNode(var type, var ignored) -> switch (type) {
        case BOOLEAN -> "true";
        case BYTE -> "(byte) 1";
        case SHORT -> "(short) 2";
        case CHARACTER -> "'c'";
        case INTEGER -> "3";
        case LONG -> "4L";
        case FLOAT -> "5.0f";
        case DOUBLE -> "6.0d";
      };
      case TypeExpr.RefValueNode(var type, var ignored) -> switch (type) {
        case BOOLEAN -> "Boolean.TRUE";
        case BYTE -> "Byte.valueOf((byte) 1)";
        case SHORT -> "Short.valueOf((short) 2)";
        case CHARACTER -> "Character.valueOf('c')";
        case INTEGER -> "Integer.valueOf(3)";
        case LONG -> "Long.valueOf(4L)";
        case FLOAT -> "Float.valueOf(5.0f)";
        case DOUBLE -> "Double.valueOf(6.0d)";
        case STRING -> "\"hello\"";
        case UUID -> "UUID.fromString(\"00000000-0000-0000-0000-000000000001\")";
        case ENUM -> "io.github.simbo1905.no.framework.ExhaustiveTest.TestEnum.A";
        case RECORD -> "new io.github.simbo1905.no.framework.ExhaustiveTest.TestRecord(123)";
        case INTERFACE -> "null"; // Cannot instantiate interface
      };
      case TypeExpr.ArrayNode(var element) -> {
        if (element instanceof TypeExpr.ArrayNode(var innerElement)) {
          if (innerElement instanceof TypeExpr.ListNode) {
            yield "createListArray2D(" + generateInstanceValue(innerElement) + ")";
          }
          if (innerElement instanceof TypeExpr.OptionalNode) {
            yield "createOptionalArray2D(" + generateInstanceValue(innerElement) + ")";
          }
          if (innerElement instanceof TypeExpr.MapNode) {
            yield "createMapArray2D(" + generateInstanceValue(innerElement) + ")";
          }
        }
        if (element instanceof TypeExpr.PrimitiveValueNode) {
          yield "new " + toJavaType(element, true) + "[]{" + generateInstanceValue(element) + "}";
        }
        if (element instanceof TypeExpr.ListNode) {
          yield "createListArray(" + generateInstanceValue(element) + ")";
        }
        if (element instanceof TypeExpr.OptionalNode) {
          yield "createOptionalArray(" + generateInstanceValue(element) + ")";
        }
        if (element instanceof TypeExpr.MapNode) {
          yield "createMapArray(" + generateInstanceValue(element) + ")";
        }
        yield "new " + toJavaType(element) + "[]{" + generateInstanceValue(element) + "}";
      }
      case TypeExpr.ListNode(var element) -> {
        String elementInstance = generateInstanceValue(element);
        if (element instanceof TypeExpr.ArrayNode) {
          yield "List.of(" + elementInstance + ", " + elementInstance + ")";
        }
        yield "List.of(" + elementInstance + ")";
      }
      case TypeExpr.OptionalNode(var wrapped) -> "Optional.of(" + generateInstanceValue(wrapped) + ")";
      case TypeExpr.MapNode(var key, var value) ->
          "Map.of(" + generateInstanceValue(key) + ", " + generateInstanceValue(value) + ")";
    };
  }

  void assertDeepEquals(Object expected, Object actual) throws Exception {
    if (expected == null || actual == null) {
      assertEquals(expected, actual);
      return;
    }

    Class<?> recordClass = expected.getClass();
    if (!recordClass.isRecord()) {
      Class<?> expectedClass = expected.getClass();
      if (!expectedClass.equals(actual.getClass())) {
        throw new IllegalArgumentException("Expected and actual objects are not of the same class: " +
            expectedClass.getName() + " vs " + actual.getClass().getName());
      } else if (expectedClass.isArray()) {
        assertArrayEquals((Object[]) expected, (Object[]) actual, "Arrays differ");
        return;
      } else if (expected instanceof List) {
        List<?> expectedList = (List<?>) expected;
        List<?> actualList = (List<?>) actual;
        assertEquals(expectedList.size(), actualList.size(), "List size differs");
        for (int i = 0; i < expectedList.size(); i++) {
          assertDeepEquals(expectedList.get(i), actualList.get(i));
        }
        return;
      } else if (expected instanceof Map) {
        Map<?, ?> expectedMap = (Map<?, ?>) expected;
        Map<?, ?> actualMap = (Map<?, ?>) actual;
        assertEquals(expectedMap.size(), actualMap.size(), "Map size differs");
        for (Object key : expectedMap.keySet()) {
          assertDeepEquals(expectedMap.get(key), actualMap.get(key));
        }
        return;
      } else {
        assertEquals(expected, actual, "Objects differ");
        return;
      }
    }

    for (RecordComponent component : recordClass.getRecordComponents()) {
      Method accessor = component.getAccessor();
      Object expectedValue = accessor.invoke(expected);
      Object actualValue = accessor.invoke(actual);

      if (component.getType().isArray()) {
        // This is a simplified array comparison. For a real-world scenario, a recursive deep equals for arrays of any dimension would be better.
        if (expectedValue instanceof Object[] && actualValue instanceof Object[]) {
          assertArrayEquals((Object[]) expectedValue, (Object[]) actualValue);
        } else {
          // fallback for primitive arrays
          assertEquals(Array.getLength(expectedValue), Array.getLength(actualValue));
          for (int i = 0; i < Array.getLength(expectedValue); i++) {
            assertEquals(Array.get(expectedValue, i), Array.get(actualValue, i));
          }
        }
      } else if (expectedValue instanceof List && actualValue instanceof List<?>) {
        List<?> expectedList = (List<?>) expectedValue;
        List<?> actualList = (List<?>) actualValue;
        assertEquals(expectedList.size(), actualList.size(), "List size differs for component " + component.getName());
        for (int i = 0; i < expectedList.size(); i++) {
          assertDeepEquals(expectedList.get(i), actualList.get(i));
        }
      } else if (expectedValue instanceof Optional<?> && actualValue instanceof Optional<?>) {
        Optional<?> expectedOptional = (Optional<?>) expectedValue;
        Optional<?> actualOptional = (Optional<?>) actualValue;
        assertEquals(expectedOptional.isPresent(), actualOptional.isPresent(), "Optional presence differs for component " + component.getName());
        if (expectedOptional.isPresent()) {
          assertDeepEquals(expectedOptional.get(), actualOptional.get());
        }
      } else if (expectedValue instanceof Map<?, ?> && actualValue instanceof Map<?, ?>) {
        Map<?, ?> expectedMap = (Map<?, ?>) expectedValue;
        Map<?, ?> actualMap = (Map<?, ?>) actualValue;
        assertEquals(expectedMap.size(), actualMap.size(), "Map size differs for component " + component.getName());
        for (Object key : expectedMap.keySet()) {
          assertDeepEquals(expectedMap.get(key), actualMap.get(key));
        }
      } else if (component.getType().isRecord()) {
        assertDeepEquals(expectedValue, actualValue);
      } else {
        assertEquals(expectedValue, actualValue, "Component " + component.getName() + " differs");
      }
    }
  }


  @SafeVarargs
  @SuppressWarnings("varargs")
  public static <T> List<T>[] createListArray(List<T>... lists) {
    return lists;
  }

  @SafeVarargs
  @SuppressWarnings("varargs")
  public static <T> List<T>[][] createListArray2D(List<T>... lists) {
    @SuppressWarnings("unchecked")
    List<T>[][] result = (List<T>[][]) new List[1][];
    result[0] = lists;
    return result;
  }


  @SafeVarargs
  @SuppressWarnings("varargs")
  public static <T> Optional<T>[][] createOptionalArray2D(Optional<T>... optionals) {
    @SuppressWarnings("unchecked")
    Optional<T>[][] result = (Optional<T>[][]) new Optional[1][];
    result[0] = optionals;
    return result;
  }

  @SafeVarargs
  @SuppressWarnings("varargs")
  public static <K, V> Map<K, V>[][] createMapArray2D(Map<K, V>... maps) {
    @SuppressWarnings("unchecked")
    Map<K, V>[][] result = (Map<K, V>[][]) new Map[1][];
    result[0] = maps;
    return result;
  }


  @SafeVarargs
  @SuppressWarnings("varargs")
  public static <T> Optional<T>[] createOptionalArray(Optional<T>... optionals) {
    return optionals;
  }

  @SafeVarargs
  @SuppressWarnings("varargs")
  public static <K, V> Map<K, V>[] createMapArray(Map<K, V>... maps) {
    return maps;
  }

}

