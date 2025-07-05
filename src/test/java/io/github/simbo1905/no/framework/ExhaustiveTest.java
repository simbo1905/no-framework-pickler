package io.github.simbo1905.no.framework;

import net.jqwik.api.*;
import net.jqwik.api.providers.ArbitraryProvider;
import net.jqwik.api.providers.TypeUsage;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
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
        doubleContainers(valueTypes),         // Depth 2: Container(Container(Value))
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
        singleContainers.map(TypeExpr.ListNode::new),       // List(Container(Value))
        singleContainers.map(TypeExpr.OptionalNode::new),   // Optional(Container(Value))
        singleContainers.map(s -> new TypeExpr.MapNode(new TypeExpr.RefValueNode(TypeExpr.RefValueType.STRING, String.class), s))
    );
  }

  private Arbitrary<TypeExpr> tripleContainers(Arbitrary<TypeExpr> valueTypes) {
    // Only essential patterns to avoid combinatorial explosion
    Arbitrary<TypeExpr> essentialDouble = Arbitraries.oneOf(
        valueTypes.map(v -> new TypeExpr.ArrayNode(new TypeExpr.ListNode(v))),           // Array(List(Value))
        valueTypes.map(v -> new TypeExpr.ListNode(new TypeExpr.ArrayNode(v))),           // List(Array(Value))
        valueTypes.map(v -> new TypeExpr.OptionalNode(new TypeExpr.ArrayNode(v))),       // Optional(Array(Value))
        valueTypes.map(v -> new TypeExpr.ListNode(new TypeExpr.OptionalNode(v)))         // List(Optional(Value))
    );

    return essentialDouble.map(TypeExpr.ArrayNode::new);  // Array(essential double combinations)
  }

  @Property(generation = GenerationMode.EXHAUSTIVE)
  @SuppressWarnings("unchecked")
  void exhaustiveGenerateAllClassesAsSrcWithRecordClassAndInstanceThenRoundTrip(@ForAll("typeExprs") TypeExpr typeExpr) throws Exception {
    String recordName = "GeneratedRecord";
    String fullClassName = "io.github.simbo1905.no.framework.generated." + recordName;

    String sourceCode = generateRecordSource(recordName, typeExpr);
    log.info("Generated source for " + typeExpr.toTreeString() + ":\n" + sourceCode);

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

    return String.format("""
        package io.github.simbo1905.no.framework.generated;
        
        import java.util.*;
        import io.github.simbo1905.no.framework.ExhaustiveTest.TestEnum;
        import io.github.simbo1905.no.framework.ExhaustiveTest.TestRecord;
        import io.github.simbo1905.no.framework.TestableRecord;
        
        public record %s(%s value) implements TestableRecord {
            public %s() { 
                this(null);
            }
        
            public Object instance() {
                return new %s(%s);
            }
        }
        """, recordName, typeName, recordName, recordName, instanceValue);
  }

  private String toJavaType(TypeExpr typeExpr) {
    return switch (typeExpr) {
      case TypeExpr.PrimitiveValueNode(var type, var javaType) -> ((Class<?>) javaType).getName();
      case TypeExpr.RefValueNode(var type, var javaType) -> {
        Class<?> clazz = (Class<?>) javaType;
        if (clazz.equals(TestRecord.class) || clazz.equals(TestEnum.class)) {
          yield clazz.getCanonicalName();
        }
        yield clazz.getSimpleName();
      }
      case TypeExpr.ArrayNode(var element) -> toJavaType(element) + "[]";
      case TypeExpr.ListNode(var element) -> "List<" + toJavaType(element) + ">";
      case TypeExpr.OptionalNode(var wrapped) -> "Optional<" + toJavaType(wrapped) + ">";
      case TypeExpr.MapNode(var key, var value) -> "Map<" + toJavaType(key) + ", " + toJavaType(value) + ">";
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
      case TypeExpr.ArrayNode(var element) ->
          "new " + toJavaType(element) + "[]{" + generateInstanceValue(element) + "}";
      case TypeExpr.ListNode(var element) -> "List.of(" + generateInstanceValue(element) + ")";
      case TypeExpr.OptionalNode(var wrapped) -> "Optional.of(" + generateInstanceValue(wrapped) + ")";
      case TypeExpr.MapNode(var key, var value) ->
          "Map.of(" + generateInstanceValue(key) + ", " + generateInstanceValue(value) + ")";
    };
  }

  private void assertDeepEquals(Object expected, Object actual) throws Exception {
    if (expected == null || actual == null) {
      assertEquals(expected, actual);
      return;
    }

    Class<?> recordClass = expected.getClass();
    if (!recordClass.isRecord()) {
      assertEquals(expected, actual);
      return;
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
      } else if (component.getType().isRecord()) {
        assertDeepEquals(expectedValue, actualValue);
      } else {
        assertEquals(expectedValue, actualValue, "Component " + component.getName() + " differs");
      }
    }
  }
}
