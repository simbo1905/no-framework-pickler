// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.*;

import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.util.Arrays;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NestedArraysTypeExprTest {

  @BeforeEach
  void setUp() {
    // Setup logic if needed
  }

  @AfterEach
  void tearDown() {
    // Tear down logic if needed
  }

  // Fields for array testing
  public int[] anIntArrayOne = {1, 2, 3};
  public int[][] anIntArrayTwo = {{1}, {2, 3}};
  public int[][][] anIntArrayThree = {{{1}}, {{2}, {3, 4}}};

  public String[] aStringArrayOne = {"a", "b", "c"};
  public String[][] aStringArrayTwo = {{"a"}, {"b", "c"}};
  public String[][][] aStringArrayThree = {{{"a"}}, {{"b"}, {"c", "d"}}};

  @Nested
  @DisplayName("Array Dimension Tests")
  class ArrayDimensionTests {

    @Test
    @DisplayName("3D int array dimensions")
    void test3DIntArrayDimensions() {
      TypeExpr node = TypeExpr.analyze(int[][][].class);
      // Test implementation will go here
      assertTrue(false, "NOT YET IMPLEMENTED");
    }

    @Test
    @DisplayName("3D String array dimensions")
    void test3DStringArrayDimensions() throws NoSuchFieldException {
      Type arrayType = NestedArraysTypeExprTest.class.getDeclaredField("aStringArrayThree").getGenericType();
      TypeExpr expected = new TypeExpr.ArrayNode(
          new TypeExpr.ArrayNode(
              new TypeExpr.ArrayNode(
                  new TypeExpr.RefValueNode(TypeExpr.RefValueType.STRING, String.class)
              )
          )
      );
      TypeExpr actual = TypeExpr.analyze(arrayType);
      assertEquals(expected, actual);
      assertThat(actual.toTreeString()).isEqualTo("ARRAY(ARRAY(ARRAY(String)))");

      final int dimensionsActual = getArrayDimensions(actual);
      assertThat(dimensionsActual).isEqualTo(3);

      final TypeExpr innerReferenceType = getArrayInnerType(actual);
      assertThat(innerReferenceType).isEqualTo(
          new TypeExpr.RefValueNode(TypeExpr.RefValueType.STRING, String.class)
      );

      final Object reflectivelyCreated = createArray(String.class, 3);
      assertThat(reflectivelyCreated.getClass()).isEqualTo(String[][][].class);

      final var sourceArray = NestedArraysTypeExprTest.this.aStringArrayThree;
      final Object fullyPopulated = createAndPopulateArray(sourceArray, actual, String.class);

      // Implement array comparison logic
      assertTrue(Arrays.deepEquals(sourceArray, (String[][][]) fullyPopulated));
    }

    @Test
    @DisplayName("2D int array dimensions")
    void test2DIntArrayDimensions() {
      TypeExpr node = TypeExpr.analyze(int[][].class);
      assertTrue(false, "NOT YET IMPLEMENTED");
    }

    @Test
    @DisplayName("2D String array dimensions")
    void test2DStringArrayDimensions() {
      TypeExpr node = TypeExpr.analyze(String[][].class);
      assertTrue(false, "NOT YET IMPLEMENTED");
    }

    @Test
    @DisplayName("1D int array dimensions")
    void test1DIntArrayDimensions() {
      TypeExpr node = TypeExpr.analyze(int[].class);
      assertTrue(false, "NOT YET IMPLEMENTED");
    }

    @Test
    @DisplayName("1D String array dimensions")
    void test1DStringArrayDimensions() {
      TypeExpr node = TypeExpr.analyze(String[].class);
      assertTrue(false, "NOT YET IMPLEMENTED");
    }
  }

  private int getArrayDimensions(TypeExpr arrayType) {
    if (!(arrayType instanceof TypeExpr.ArrayNode)) {
      throw new IllegalArgumentException("Not an array type");
    }
    final var r = 1 + getArrayDimensions(((TypeExpr.ArrayNode) arrayType).element());
    LOGGER.finer(() -> "Array dimensions of " + arrayType.toTreeString() + "=" + r);
    return r;
  }

  private TypeExpr getArrayInnerType(TypeExpr arrayType) {
    if (!(arrayType instanceof TypeExpr.ArrayNode)) {
      return arrayType;
    }
    return getArrayInnerType(((TypeExpr.ArrayNode) arrayType).element());
  }

  private Object createArray(Class<?> componentType, int dimensions) {
    int[] dims = new int[dimensions];
    Arrays.fill(dims, 1); // Default size 1 for each dimension
    return Array.newInstance(componentType, dims);
  }

  private Object createAndPopulateArray(Object sourceArray, TypeExpr typeExpr, Class<?> javaType) {
    int length = Array.getLength(sourceArray);
    Object newArray = Array.newInstance(
        typeExpr instanceof TypeExpr.ArrayNode ?
            javaType :
            Object.class,
        length
    );

    for (int i = 0; i < length; i++) {
      Object element = Array.get(sourceArray, i);
      assert typeExpr instanceof TypeExpr.ArrayNode;
      Array.set(newArray, i, createAndPopulateArray(element, ((TypeExpr.ArrayNode) typeExpr).element(), javaType));
    }
    return newArray;
  }
}
