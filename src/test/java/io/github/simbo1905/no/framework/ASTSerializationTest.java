// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test suite for AST-based recursive serialization and deserialization.
 * This class demonstrates the core recursive algorithm for building delegation chains
 * from TypeExpr AST structures.
 */
@SuppressWarnings({"unused", "OptionalUsedAsFieldOrParameterType"}) // Test fields accessed via reflection
class ASTSerializationTest {

  // Test data fields - accessed via reflection to get generic types
  private final List<String> stringList = List.of("hello", "world", "test");
  private final Optional<Integer> optionalInteger = Optional.of(42);
  private final Optional<Integer> optionalEmpty = Optional.empty();
  private final String[] stringArray = {"one", "two", "three"};
  private final Integer[] integerArray = {1, 2, 3, 4, 5};
  private final int[] primitiveIntArray = {10, 20, 30};
  private final Map<String, Integer> stringIntegerMap = Map.of("a", 1, "b", 2, "c", 3);

  // Nested container test data
  private final List<List<String>> nestedStringList = List.of(
      List.of("a", "b"),
      List.of("c", "d", "e")
  );
  private final Optional<String[]> optionalStringArray = Optional.of(new String[]{"opt1", "opt2"});
  private final Map<String, List<Integer>> mapStringListInteger = Map.of(
      "first", List.of(1, 2, 3),
      "second", List.of(4, 5)
  );
  private final List<Optional<Boolean>> listOptionalBoolean = List.of(
      Optional.of(true),
      Optional.empty(),
      Optional.of(false)
  );

  // Complex academic example
  @SuppressWarnings("unchecked")
  private final List<Map<String, Optional<Integer[]>[]>> complexAcademicExample = List.of(
      Map.of("key1", new Optional[]{Optional.of(new Integer[]{1, 2}), Optional.empty()}),
      Map.of("key2", new Optional[]{Optional.of(new Integer[]{3, 4, 5})})
  );

  // Core test method that performs round-trip testing
  private <T> void testRoundTrip(String fieldName, T expectedValue) throws Exception {
    // Get the generic type from the field
    Type genericType = getClass().getDeclaredField(fieldName).getGenericType();

    // Analyze the type to build AST
    TypeExpr typeExpr = TypeExpr.analyze(genericType);
    LOGGER.fine("Testing " + fieldName + " with AST: " + typeExpr.toTreeString());

    // Get method handle for field access
    MethodHandle getter = MethodHandles.lookup()
        .unreflectGetter(getClass().getDeclaredField(fieldName));

    // Build writer and reader chains from AST
    BiConsumer<ByteBuffer, Object> writer = RecordPickler.buildWriterChainFromAST(typeExpr, getter);
    Function<ByteBuffer, Object> reader = RecordPickler.buildReaderChainFromAST(typeExpr);

    // Serialize
    ByteBuffer buffer = ByteBuffer.allocate(1024);
    writer.accept(buffer, this);
    buffer.flip();

    // Deserialize
    @SuppressWarnings("unchecked")
    T result = (T) reader.apply(buffer);

    // Verify round-trip
    assertEquals(expectedValue, result, "Round-trip failed for " + fieldName);
  }

  // Test cases

  @Test
  @DisplayName("Simple List<String>")
  void testListString() throws Exception {
    testRoundTrip("stringList", stringList);
  }

  @Test
  @DisplayName("Optional<Integer> with value")
  void testOptionalInteger() throws Exception {
    testRoundTrip("optionalInteger", optionalInteger);
  }

  @Test
  @DisplayName("Optional<Integer> empty")
  void testOptionalEmpty() throws Exception {
    testRoundTrip("optionalEmpty", optionalEmpty);
  }

  @Test
  @DisplayName("String array")
  void testStringArray() throws Exception {
    testRoundTrip("stringArray", stringArray);
  }

  @Test
  @DisplayName("Integer array (reference type)")
  void testIntegerArray() throws Exception {
    testRoundTrip("integerArray", integerArray);
  }

  @Test
  @DisplayName("int array (primitive type)")
  void testPrimitiveIntArray() throws Exception {
    testRoundTrip("primitiveIntArray", primitiveIntArray);
  }

  @Test
  @DisplayName("Map<String, Integer>")
  void testStringIntegerMap() throws Exception {
    testRoundTrip("stringIntegerMap", stringIntegerMap);
  }

  @Test
  @DisplayName("Nested List<List<String>>")
  void testNestedStringList() throws Exception {
    testRoundTrip("nestedStringList", nestedStringList);
  }

  @Test
  @DisplayName("Optional<String[]>")
  void testOptionalStringArray() throws Exception {
    testRoundTrip("optionalStringArray", optionalStringArray);
  }

  @Test
  @DisplayName("Map<String, List<Integer>>")
  void testMapStringListInteger() throws Exception {
    testRoundTrip("mapStringListInteger", mapStringListInteger);
  }

  @Test
  @DisplayName("List<Optional<Boolean>>")
  void testListOptionalBoolean() throws Exception {
    testRoundTrip("listOptionalBoolean", listOptionalBoolean);
  }

  @Test
  @DisplayName("Complex Academic Example: List<Map<String, Optional<Integer[]>[]>>")
  void testComplexAcademicExample() throws Exception {
    testRoundTrip("complexAcademicExample", complexAcademicExample);
  }
}
