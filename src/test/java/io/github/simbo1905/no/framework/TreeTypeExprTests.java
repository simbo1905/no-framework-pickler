// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.*;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/// Test suite for the new tree-based TypeExpr structure
/// Following TDD: RED phase - these tests define expected behavior
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
class TreeTypeExprTests {

  @BeforeEach
  void setUp() {
    LOGGER.fine(() -> "Starting tree TypeExpr test");
  }

  @AfterEach
  void tearDown() {
    LOGGER.fine(() -> "Finished tree TypeExpr test");
  }

  @Nested
  @DisplayName("Primitive Type Tests")
  class PrimitiveTypeTests {

    @Test
    @DisplayName("Boolean primitive")
    void testBooleanPrimitive() {
      TypeExpr node = TypeExpr.analyzeType(boolean.class);
      assertInstanceOf(TypeExpr.PrimitiveValueNode.class, node);
      TypeExpr.PrimitiveValueNode prim = (TypeExpr.PrimitiveValueNode) node;
      assertEquals(TypeExpr.PrimitiveValueType.BOOLEAN, prim.type());
      assertEquals(boolean.class, prim.javaType());
      assertEquals("boolean", node.toTreeString());
    }

    @Test
    @DisplayName("Boolean boxed")
    void testBooleanBoxed() {
      TypeExpr node = TypeExpr.analyzeType(Boolean.class);
      assertInstanceOf(TypeExpr.RefValueNode.class, node);
      TypeExpr.RefValueNode prim = (TypeExpr.RefValueNode) node;
      assertEquals(TypeExpr.RefValueType.BOOLEAN, prim.type());
      assertEquals(Boolean.class, prim.javaType());
      assertEquals(Boolean.class.getSimpleName(), node.toTreeString());
    }

    @Test
    @DisplayName("Integer primitive")
    void testIntegerPrimitive() {
      TypeExpr node = TypeExpr.analyzeType(int.class);
      assertInstanceOf(TypeExpr.PrimitiveValueNode.class, node);
      TypeExpr.PrimitiveValueNode prim = (TypeExpr.PrimitiveValueNode) node;
      assertEquals(TypeExpr.PrimitiveValueType.INTEGER, prim.type());
      assertEquals(int.class, prim.javaType());
      assertEquals("int", node.toTreeString());
    }

    @Test
    @DisplayName("Integer boxed")
    void testIntegerBoxed() {
      TypeExpr node = TypeExpr.analyzeType(Integer.class);
      assertInstanceOf(TypeExpr.RefValueNode.class, node);
      TypeExpr.RefValueNode prim = (TypeExpr.RefValueNode) node;
      assertEquals(TypeExpr.RefValueType.INTEGER, prim.type());
      assertEquals(Integer.class, prim.javaType());
      assertEquals("Integer", node.toTreeString());
    }

    @Test
    @DisplayName("String type")
    void testStringType() {
      TypeExpr node = TypeExpr.analyzeType(String.class);

      assertInstanceOf(TypeExpr.RefValueNode.class, node);
      TypeExpr.RefValueNode prim = (TypeExpr.RefValueNode) node;
      assertEquals(TypeExpr.RefValueType.STRING, prim.type());
      assertEquals(String.class, prim.javaType());
      assertEquals("String", node.toTreeString());
    }

    @Test
    @DisplayName("UUID type")
    void testUUIDType() {
      TypeExpr node = TypeExpr.analyzeType(UUID.class);

      assertInstanceOf(TypeExpr.RefValueNode.class, node);
      TypeExpr.RefValueNode prim = (TypeExpr.RefValueNode) node;
      assertEquals(TypeExpr.RefValueType.UUID, prim.type());
      assertEquals(UUID.class, prim.javaType());
      assertEquals("UUID", node.toTreeString());
    }
  }

  @Nested
  @DisplayName("Simple Container Tests")
  class SimpleContainerTests {

    @Test
    @DisplayName("Array of int")
    void testIntArray() {
      TypeExpr node = TypeExpr.analyzeType(int[].class);

      assertInstanceOf(TypeExpr.ArrayNode.class, node);
      TypeExpr.ArrayNode array = (TypeExpr.ArrayNode) node;

      // Check element type
      assertInstanceOf(TypeExpr.PrimitiveValueNode.class, array.element());
      TypeExpr.PrimitiveValueNode elem = (TypeExpr.PrimitiveValueNode) array.element();
      assertEquals(TypeExpr.PrimitiveValueType.INTEGER, elem.type());

      assertEquals("ARRAY(int)", node.toTreeString());
    }

    @Test
    @DisplayName("Array of boxed int")
    void testIntegerArray() {
      TypeExpr node = TypeExpr.analyzeType(Integer[].class);

      assertInstanceOf(TypeExpr.ArrayNode.class, node);
      TypeExpr.ArrayNode array = (TypeExpr.ArrayNode) node;

      // Check element type
      assertInstanceOf(TypeExpr.RefValueNode.class, array.element());
      TypeExpr.RefValueNode elem = (TypeExpr.RefValueNode) array.element();
      assertEquals(TypeExpr.RefValueType.INTEGER, elem.type());

      assertEquals("ARRAY(Integer)", node.toTreeString());
    }


    @Test
    @DisplayName("Array of String")
    void testStringArray() {
      TypeExpr node = TypeExpr.analyzeType(String[].class);

      assertInstanceOf(TypeExpr.ArrayNode.class, node);
      TypeExpr.ArrayNode array = (TypeExpr.ArrayNode) node;

      assertInstanceOf(TypeExpr.RefValueNode.class, array.element());
      TypeExpr.RefValueNode elem = (TypeExpr.RefValueNode) array.element();
      assertEquals(TypeExpr.RefValueType.STRING, elem.type());

      assertEquals("ARRAY(String)", node.toTreeString());
    }

    @Test
    @DisplayName("List of String")
    void testListString() throws Exception {
      Type listType = TreeTypeExprTests.class.getDeclaredField("stringList").getGenericType();
      TypeExpr node = TypeExpr.analyzeType(listType);

      assertInstanceOf(TypeExpr.ListNode.class, node);
      TypeExpr.ListNode list = (TypeExpr.ListNode) node;

      assertInstanceOf(TypeExpr.RefValueNode.class, list.element());
      TypeExpr.RefValueNode elem = (TypeExpr.RefValueNode) list.element();
      assertEquals(TypeExpr.RefValueType.STRING, elem.type());

      assertEquals("LIST(String)", node.toTreeString());
    }

    @Test
    @DisplayName("Optional of Integer")
    void testOptionalInteger() throws Exception {
      Type optType = TreeTypeExprTests.class.getDeclaredField("optionalInt").getGenericType();
      TypeExpr node = TypeExpr.analyzeType(optType);

      assertInstanceOf(TypeExpr.OptionalNode.class, node);
      TypeExpr.OptionalNode opt = (TypeExpr.OptionalNode) node;

      assertInstanceOf(TypeExpr.RefValueNode.class, opt.wrapped());
      TypeExpr.RefValueNode wrapped = (TypeExpr.RefValueNode) opt.wrapped();
      assertEquals(TypeExpr.RefValueType.INTEGER, wrapped.type());

      assertEquals("OPTIONAL(Integer)", node.toTreeString());
    }
  }

  @Nested
  @DisplayName("Map Tests")
  class MapTests {

    @Test
    @DisplayName("Map String to Integer")
    void testMapStringInteger() throws Exception {
      Type mapType = TreeTypeExprTests.class.getDeclaredField("stringIntMap").getGenericType();
      TypeExpr node = TypeExpr.analyzeType(mapType);

      assertInstanceOf(TypeExpr.MapNode.class, node);
      TypeExpr.MapNode map = (TypeExpr.MapNode) node;

      // Check key type
      assertInstanceOf(TypeExpr.RefValueNode.class, map.key());
      TypeExpr.RefValueNode key = (TypeExpr.RefValueNode) map.key();
      assertEquals(TypeExpr.RefValueType.STRING, key.type());

      // Check value type
      assertInstanceOf(TypeExpr.RefValueNode.class, map.value());
      TypeExpr.RefValueNode value = (TypeExpr.RefValueNode) map.value();
      assertEquals(TypeExpr.RefValueType.INTEGER, value.type());

      assertEquals("MAP(String,Integer)", node.toTreeString());
    }
  }

  @Nested
  @DisplayName("Nested Container Tests")
  class NestedContainerTests {

    @Test
    @DisplayName("List of Optional String")
    void testListOptionalString() throws Exception {
      Type type = TreeTypeExprTests.class.getDeclaredField("listOptionalString").getGenericType();
      TypeExpr node = TypeExpr.analyzeType(type);

      assertInstanceOf(TypeExpr.ListNode.class, node);
      TypeExpr.ListNode list = (TypeExpr.ListNode) node;

      assertInstanceOf(TypeExpr.OptionalNode.class, list.element());
      TypeExpr.OptionalNode opt = (TypeExpr.OptionalNode) list.element();

      assertInstanceOf(TypeExpr.RefValueNode.class, opt.wrapped());
      TypeExpr.RefValueNode str = (TypeExpr.RefValueNode) opt.wrapped();
      assertEquals(TypeExpr.RefValueType.STRING, str.type());

      assertEquals("LIST(OPTIONAL(String))", node.toTreeString());
    }

    @Test
    @DisplayName("Map with List values")
    void testMapStringListInteger() throws Exception {
      Type type = TreeTypeExprTests.class.getDeclaredField("mapStringListInt").getGenericType();
      TypeExpr node = TypeExpr.analyzeType(type);

      assertInstanceOf(TypeExpr.MapNode.class, node);
      TypeExpr.MapNode map = (TypeExpr.MapNode) node;

      // Key is String
      assertInstanceOf(TypeExpr.RefValueNode.class, map.key());
      assertEquals(TypeExpr.RefValueType.STRING, ((TypeExpr.RefValueNode) map.key()).type());

      // Value is List<Integer>
      assertInstanceOf(TypeExpr.ListNode.class, map.value());
      TypeExpr.ListNode list = (TypeExpr.ListNode) map.value();
      assertInstanceOf(TypeExpr.RefValueNode.class, list.element());
      assertEquals(TypeExpr.RefValueType.INTEGER, ((TypeExpr.RefValueNode) list.element()).type());

      assertEquals("MAP(String,LIST(Integer))", node.toTreeString());
    }

    @Test
    @DisplayName("List of double[]")
    void testListArrayDouble() throws NoSuchFieldException {
      // listArrayPrimitiveDouble
      Type type = TreeTypeExprTests.class.getDeclaredField("listArrayPrimitiveDouble").getGenericType();
      TypeExpr node = TypeExpr.analyzeType(type);

      assertInstanceOf(TypeExpr.ListNode.class, node);
      TypeExpr.ListNode list = (TypeExpr.ListNode) node;

      // Value is List<double[]>
      assertInstanceOf(TypeExpr.ListNode.class, list);
      assertInstanceOf(TypeExpr.ArrayNode.class, list.element());
      final var array = (TypeExpr.ArrayNode) list.element();
      // Check the array is of primitive double
      assertInstanceOf(TypeExpr.PrimitiveValueNode.class, array.element());
      final var elem = (TypeExpr.PrimitiveValueNode) array.element();
      assertEquals(TypeExpr.PrimitiveValueType.DOUBLE, elem.type());

      assertEquals("LIST(ARRAY(double))", node.toTreeString());
    }
  }

  @Nested
  @DisplayName("User-Defined Type Tests")
  class UserDefinedTypeTests {

    @Test
    @DisplayName("Enum type")
    void testEnumType() {
      TypeExpr node = TypeExpr.analyzeType(TestEnum.class);

      assertInstanceOf(TypeExpr.RefValueNode.class, node);
      TypeExpr.RefValueNode prim = (TypeExpr.RefValueNode) node;
      assertEquals(TypeExpr.RefValueType.ENUM, prim.type());
      assertEquals(TestEnum.class, prim.javaType());
      assertEquals(TestEnum.class.getSimpleName(), node.toTreeString());
    }

    @Test
    @DisplayName("Record type")
    void testRecordType() {
      TypeExpr node = TypeExpr.analyzeType(TestRecord.class);

      assertInstanceOf(TypeExpr.RefValueNode.class, node);
      TypeExpr.RefValueNode prim = (TypeExpr.RefValueNode) node;
      assertEquals(TypeExpr.RefValueType.RECORD, prim.type());
      assertEquals(TestRecord.class, prim.javaType());
      assertEquals(TestRecord.class.getSimpleName(), node.toTreeString());
    }
  }

  // Test types for reflection
  List<String> stringList;
  Optional<Integer> optionalInt;
  Map<String, Integer> stringIntMap;
  List<Optional<String>> listOptionalString;
  Map<String, List<Integer>> mapStringListInt;
  List<double[]> listArrayPrimitiveDouble;

  @SuppressWarnings("unused")
  enum TestEnum {ONE, TWO}

  record TestRecord(String name, int value) {
  }

  @Nested
  @DisplayName("Readme Specific Type Tests")
  class ReadmeSpecificExamplesTest {

    @BeforeEach
    void setUp() {
      // Cache not implemented in TypeExpr
    }

    @AfterEach
    void tearDown() {
      // Cache not implemented in TypeExpr
    }

    TypeExpr intType = TypeExpr.analyzeType(int.class);

    TypeExpr booleanType = TypeExpr.analyzeType(boolean.class);

    // ===============================
    // Example 1: Basic Record and Enum (Month/Season)
    // ===============================

    @SuppressWarnings("unused")
    public record Month(Season season, String name) {
    }

    @SuppressWarnings("unused")
    public enum Season {SPRING, SUMMER, FALL, WINTER}

    @Test
    @DisplayName("Month Record with Season Enum")
    void testMonthSeasonExample() {
      // Test the Season enum directly
      TypeExpr seasonEnum = TypeExpr.analyzeType(Season.class);
      assertEquals(new TypeExpr.RefValueNode(TypeExpr.RefValueType.ENUM, Season.class), seasonEnum);

      // Test String type
      TypeExpr stringType = TypeExpr.analyzeType(String.class);
      assertEquals(new TypeExpr.RefValueNode(TypeExpr.RefValueType.STRING, String.class), stringType);
    }

    // ===============================
    // Example 2: TreeNode Sealed Interface Hierarchy
    // ===============================

    public sealed interface TreeNode permits InternalNode, LeafNode, TreeEnum {
      @SuppressWarnings("unused")
      static TreeNode empty() {
        return TreeEnum.EMPTY;
      }
    }

    public record InternalNode(String name, TreeNode left, TreeNode right) implements TreeNode {
    }

    public record LeafNode(int value) implements TreeNode {
    }

    public enum TreeEnum implements TreeNode {EMPTY}

    @Test
    @DisplayName("TreeNode Sealed Interface")
    void testTreeNodeSealedInterface() {
      // Test the sealed interface itself
      TypeExpr treeNodeInterface = TypeExpr.analyzeType(TreeNode.class);
      assertEquals(new TypeExpr.RefValueNode(TypeExpr.RefValueType.INTERFACE, TreeNode.class), treeNodeInterface);

      // Test record implementing interface
      TypeExpr internalNodeRecord = TypeExpr.analyzeType(InternalNode.class);
      assertEquals(new TypeExpr.RefValueNode(TypeExpr.RefValueType.RECORD, InternalNode.class), internalNodeRecord);

      // Test enum implementing interface
      TypeExpr treeEnum = TypeExpr.analyzeType(TreeEnum.class);
      assertEquals(new TypeExpr.RefValueNode(TypeExpr.RefValueType.ENUM, TreeEnum.class), treeEnum);
    }

    @Test
    @DisplayName("InternalNode Record Structure")
    void testInternalNodeStructure() {
      // Test the types of fields in InternalNode
      // name: String
      TypeExpr stringType = TypeExpr.analyzeType(String.class);
      assertEquals(new TypeExpr.RefValueNode(TypeExpr.RefValueType.STRING, String.class), stringType);

      // left/right: TreeNode (recursive reference to sealed interface)
      TypeExpr treeNodeType = TypeExpr.analyzeType(TreeNode.class);
      assertEquals(new TypeExpr.RefValueNode(TypeExpr.RefValueType.INTERFACE, TreeNode.class), treeNodeType);
    }

    @Test
    @DisplayName("LeafNode Record Structure")
    void testLeafNodeStructure() {
      // Test int type for value field
      TypeExpr intType = TypeExpr.analyzeType(int.class);
      assertEquals(new TypeExpr.PrimitiveValueNode(TypeExpr.PrimitiveValueType.INTEGER, int.class), intType);
    }

    // ===============================
    // Example 3: Nested List Records (NestedListRecord)
    // ===============================

    public record NestedListRecord(List<List<String>> nestedList) {
    }

    @Test
    @DisplayName("NestedListRecord Structure")
    void testNestedListRecordStructure() throws Exception {
      Type nestedListType = NestedListRecord.class.getDeclaredField("nestedList").getGenericType();

      // Build expected tree: LIST(LIST(STRING))
      TypeExpr expected = new TypeExpr.ListNode(
          new TypeExpr.ListNode(
              new TypeExpr.RefValueNode(TypeExpr.RefValueType.STRING, String.class)
          )
      );

      TypeExpr actual = TypeExpr.analyzeType(nestedListType);
      assertEquals(expected, actual);
      assertEquals("LIST(LIST(String))", actual.toTreeString());
    }

    // ===============================
    // Example 4: Person and Family Map Records
    // ===============================

    public record Person(String name, int age) {
    }

    public record NestedFamilyMapContainer(Person subject, Map<String, Person> relationships) {
    }

    @Test
    @DisplayName("Person Record Structure")
    void testPersonRecordStructure() {
      // Test the types used in Person record
      TypeExpr stringType = TypeExpr.analyzeType(String.class);
      assertEquals(new TypeExpr.RefValueNode(TypeExpr.RefValueType.STRING, String.class), stringType);

      TypeExpr intType = TypeExpr.analyzeType(int.class);
      assertEquals(new TypeExpr.PrimitiveValueNode(TypeExpr.PrimitiveValueType.INTEGER, int.class), intType);
    }

    @Test
    @DisplayName("NestedFamilyMapContainer Structure")
    void testNestedFamilyMapContainerStructure() throws Exception {
      // subject: Person (record)
      TypeExpr personType = TypeExpr.analyzeType(Person.class);
      assertEquals(new TypeExpr.RefValueNode(TypeExpr.RefValueType.RECORD, Person.class), personType);

      // relationships: Map<String,Person>
      Type mapType = NestedFamilyMapContainer.class.getDeclaredField("relationships").getGenericType();
      TypeExpr expected = new TypeExpr.MapNode(
          new TypeExpr.RefValueNode(TypeExpr.RefValueType.STRING, String.class),
          new TypeExpr.RefValueNode(TypeExpr.RefValueType.RECORD, Person.class)
      );
      TypeExpr actual = TypeExpr.analyzeType(mapType);
      assertEquals(expected, actual);
      assertEquals("MAP(String,Person)", actual.toTreeString());
    }

    // ===============================
    // Example 5: Complex Animal Hierarchy
    // ===============================

    public sealed interface Animal permits Mammal, Bird, Alicorn {
    }

    public sealed interface Mammal extends Animal permits Dog, Cat {
    }

    public sealed interface Bird extends Animal permits Eagle, Penguin {
    }

    public record Alicorn(String name, String[] magicPowers) implements Animal {
    }

    public record Dog(String name, int age) implements Mammal {
    }

    public record Cat(String name, boolean purrs) implements Mammal {
    }

    public record Eagle(double wingspan) implements Bird {
    }

    public record Penguin(boolean canSwim) implements Bird {
    }

    @Test
    @DisplayName("Animal Sealed Hierarchy Analysis")
    void testAnimalSealedHierarchy() {
      // Test various types in the hierarchy
      TypeExpr alicornType = TypeExpr.analyzeType(Alicorn.class);
      assertEquals(new TypeExpr.RefValueNode(TypeExpr.RefValueType.RECORD, Alicorn.class), alicornType);

      TypeExpr mammalType = TypeExpr.analyzeType(Mammal.class);
      assertEquals(new TypeExpr.RefValueNode(TypeExpr.RefValueType.INTERFACE, Mammal.class), mammalType);

      TypeExpr dogType = TypeExpr.analyzeType(Dog.class);
      assertEquals(new TypeExpr.RefValueNode(TypeExpr.RefValueType.RECORD, Dog.class), dogType);
    }

    @Test
    @DisplayName("Alicorn Record with String Array")
    void testAlicornRecordStructure() throws Exception {
      // name: String
      TypeExpr stringType = TypeExpr.analyzeType(String.class);
      assertEquals(new TypeExpr.RefValueNode(TypeExpr.RefValueType.STRING, String.class), stringType);

      // magicPowers: String[]
      Type arrayType = Alicorn.class.getDeclaredField("magicPowers").getGenericType();
      TypeExpr expected = new TypeExpr.ArrayNode(
          new TypeExpr.RefValueNode(TypeExpr.RefValueType.STRING, String.class)
      );
      TypeExpr actual = TypeExpr.analyzeType(arrayType);
      assertEquals(expected, actual);
      assertThat(actual).isEqualTo(expected);
      assertEquals("ARRAY(String)", actual.toTreeString());
    }

    @Test
    @DisplayName("Dog and Cat Record Structures")
    void testMammalRecordStructures() {
      // Test types used in Dog and Cat records
      TypeExpr stringType = TypeExpr.analyzeType(String.class);
      assertEquals(new TypeExpr.RefValueNode(TypeExpr.RefValueType.STRING, String.class), stringType);
      assertEquals(new TypeExpr.PrimitiveValueNode(TypeExpr.PrimitiveValueType.INTEGER, int.class), intType);
      assertEquals(new TypeExpr.PrimitiveValueNode(TypeExpr.PrimitiveValueType.BOOLEAN, boolean.class), booleanType);
    }

    @Test
    @DisplayName("Eagle and Penguin Record Structures")
    void testBirdRecordStructures() {
      // Test types used in Eagle and Penguin records
      TypeExpr doubleType = TypeExpr.analyzeType(double.class);
      assertEquals(new TypeExpr.PrimitiveValueNode(TypeExpr.PrimitiveValueType.DOUBLE, double.class), doubleType);

      TypeExpr booleanType = TypeExpr.analyzeType(boolean.class);
      assertEquals(new TypeExpr.PrimitiveValueNode(TypeExpr.PrimitiveValueType.BOOLEAN, boolean.class), booleanType);
    }

    // ===============================
    // Example 6: Complex Type from Academic Analysis
    // List<Map<String,Optional<Integer[]>[]>>
    // ===============================

    @Test
    @DisplayName("README Academic Example: List<Map<String,Optional<Integer[]>[]>>")
    void testComplexAcademicExample() throws Exception {
      Type complexType = getClass().getDeclaredField("complexAcademicExample").getGenericType();

      // Build expected tree from inside out: LIST(MAP(STRING, ARRAY(OPTIONAL(ARRAY(INTEGER)))))
      TypeExpr integerNode = new TypeExpr.RefValueNode(TypeExpr.RefValueType.INTEGER, Integer.class);
      TypeExpr innerArrayNode = new TypeExpr.ArrayNode(integerNode);
      TypeExpr optionalNode = new TypeExpr.OptionalNode(innerArrayNode);
      TypeExpr outerArrayNode = new TypeExpr.ArrayNode(optionalNode);
      TypeExpr stringNode = new TypeExpr.RefValueNode(TypeExpr.RefValueType.STRING, String.class);
      TypeExpr mapNode = new TypeExpr.MapNode(stringNode, outerArrayNode);
      TypeExpr expected = new TypeExpr.ListNode(mapNode);

      TypeExpr actual = TypeExpr.analyzeType(complexType);
      assertEquals(expected, actual);
      assertEquals("LIST(MAP(String,ARRAY(OPTIONAL(ARRAY(Integer)))))", actual.toTreeString());
    }

    // ===============================
    // Example 7: Wire Protocol Supported Types
    // ===============================

    @Test
    @DisplayName("README Wire Protocol: All Primitive Types")
    void testWireProtocolPrimitiveTypes() {
      // Map each type to its expected TypeExpr.PrimitiveType
      Map<Class<?>, ?> typeMapping = Map.ofEntries(
          Map.entry(boolean.class, TypeExpr.PrimitiveValueType.BOOLEAN),
          Map.entry(Boolean.class, TypeExpr.RefValueType.BOOLEAN),
          Map.entry(byte.class, TypeExpr.PrimitiveValueType.BYTE),
          Map.entry(Byte.class, TypeExpr.RefValueType.BYTE),
          Map.entry(short.class, TypeExpr.PrimitiveValueType.SHORT),
          Map.entry(Short.class, TypeExpr.RefValueType.SHORT),
          Map.entry(char.class, TypeExpr.PrimitiveValueType.CHARACTER),
          Map.entry(Character.class, TypeExpr.RefValueType.CHARACTER),
          Map.entry(int.class, TypeExpr.PrimitiveValueType.INTEGER),
          Map.entry(Integer.class, TypeExpr.RefValueType.INTEGER),
          Map.entry(long.class, TypeExpr.PrimitiveValueType.LONG),
          Map.entry(Long.class, TypeExpr.RefValueType.LONG),
          Map.entry(float.class, TypeExpr.PrimitiveValueType.FLOAT),
          Map.entry(Float.class, TypeExpr.RefValueType.FLOAT),
          Map.entry(double.class, TypeExpr.PrimitiveValueType.DOUBLE),
          Map.entry(Double.class, TypeExpr.RefValueType.DOUBLE),
          Map.entry(String.class, TypeExpr.RefValueType.STRING),
          Map.entry(UUID.class, TypeExpr.RefValueType.UUID)
      );

      for (Map.Entry<Class<?>, ?> entry : typeMapping.entrySet()) {
        Class<?> javaType = entry.getKey();
        Object expectedType = entry.getValue();
        TypeExpr expected = switch (expectedType) {
          case TypeExpr.PrimitiveValueType primitiveType -> new TypeExpr.PrimitiveValueNode(primitiveType, javaType);
          case TypeExpr.RefValueType refValueType -> new TypeExpr.RefValueNode(refValueType, javaType);
          default -> throw new IllegalArgumentException("Unexpected type: " + expectedType);
        };
        TypeExpr actual = TypeExpr.analyzeType(javaType);

        assertEquals(expected, actual, "Failed for " + javaType);
      }
    }

    @Test
    @DisplayName("README Wire Protocol: Container Types")
    void testWireProtocolContainerTypes() throws Exception {
      // Test Array: int[] -> ARRAY(INTEGER)
      TypeExpr expectedArray = new TypeExpr.ArrayNode(
          new TypeExpr.PrimitiveValueNode(TypeExpr.PrimitiveValueType.INTEGER, int.class)
      );
      TypeExpr actualArray = TypeExpr.analyzeType(int[].class);
      assertEquals(expectedArray, actualArray);
      assertEquals("ARRAY(int)", actualArray.toTreeString());

      // Test List: List<String> -> LIST(STRING)
      Type listType = getClass().getDeclaredField("stringList").getGenericType();
      TypeExpr expectedList = new TypeExpr.ListNode(
          new TypeExpr.RefValueNode(TypeExpr.RefValueType.STRING, String.class)
      );
      TypeExpr actualList = TypeExpr.analyzeType(listType);
      assertEquals(expectedList, actualList);
      assertEquals("LIST(String)", actualList.toTreeString());

      // Test Optional: Optional<String> -> OPTIONAL(STRING)
      Type optionalType = getClass().getDeclaredField("optionalString").getGenericType();
      TypeExpr expectedOptional = new TypeExpr.OptionalNode(
          new TypeExpr.RefValueNode(TypeExpr.RefValueType.STRING, String.class)
      );
      TypeExpr actualOptional = TypeExpr.analyzeType(optionalType);
      assertEquals(expectedOptional, actualOptional);
      assertEquals("OPTIONAL(String)", actualOptional.toTreeString());

      // Test Map: Map<String,Integer> -> MAP(STRING, INTEGER)
      Type mapType = getClass().getDeclaredField("stringIntMap").getGenericType();
      TypeExpr expectedMap = new TypeExpr.MapNode(
          new TypeExpr.RefValueNode(TypeExpr.RefValueType.STRING, String.class),
          new TypeExpr.RefValueNode(TypeExpr.RefValueType.INTEGER, Integer.class)
      );
      TypeExpr actualMap = TypeExpr.analyzeType(mapType);
      assertEquals(expectedMap, actualMap);
      assertEquals("MAP(String,Integer)", actualMap.toTreeString());
    }


    // Required fields for reflection-based generic type access
    @SuppressWarnings("unused")
    List<Map<String, Optional<Integer[]>[]>> complexAcademicExample;
    @SuppressWarnings("unused")
    List<String> stringList;
    @SuppressWarnings("unused")
    Optional<String> optionalString;
    @SuppressWarnings("unused")
    Map<String, Integer> stringIntMap;
  }

}
