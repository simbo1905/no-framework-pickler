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

@SuppressWarnings("unused")
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
    @DisplayName("3D String array dimensions")
    void test3DStringArrayDimensions() throws NoSuchFieldException {
      LOGGER.info(() -> "Starting test for 3D String array dimensions");
      Type arrayType = NestedArraysTypeExprTest.class.getDeclaredField("aStringArrayThree").getGenericType();
      LOGGER.finer(() -> "Analyzing array type: " + arrayType.getTypeName());

      TypeExpr expected = new TypeExpr.ArrayNode(
          new TypeExpr.ArrayNode(
              new TypeExpr.ArrayNode(
                  new TypeExpr.RefValueNode(TypeExpr.RefValueType.STRING, String.class)
              )
          )
      );
      LOGGER.finer(() -> "Expected TypeExpr: " + expected.toTreeString());

      TypeExpr actual = TypeExpr.analyze(arrayType);
      LOGGER.finer(() -> "Actual TypeExpr: " + actual.toTreeString());

      assertEquals(expected, actual);
      assertThat(actual.toTreeString()).isEqualTo("ARRAY(ARRAY(ARRAY(String)))");

      final int dimensionsActual = getArrayDimensions(actual);
      LOGGER.finer(() -> "Array dimensions: " + dimensionsActual);
      assertThat(dimensionsActual).isEqualTo(3);

      final TypeExpr innerReferenceType = getArrayInnerType(actual);
      LOGGER.finer(() -> "Inner reference type: " + innerReferenceType.toTreeString());
      assertThat(innerReferenceType).isEqualTo(
          new TypeExpr.RefValueNode(TypeExpr.RefValueType.STRING, String.class)
      );

      final Object reflectivelyCreated = createArray(String.class, 3);
      LOGGER.finer(() -> "Reflectively created array type: " + reflectivelyCreated.getClass().getTypeName());
      assertThat(reflectivelyCreated.getClass()).isEqualTo(String[][][].class);

      final var sourceArray = NestedArraysTypeExprTest.this.aStringArrayThree;
      LOGGER.finer(() -> "Source array structure: " + Arrays.deepToString(sourceArray));

      final Object fullyPopulated = createAndPopulateArray(sourceArray, actual, String.class);
      LOGGER.finer(() -> "Fully populated array type: " + fullyPopulated.getClass().getTypeName());

      // Implement array comparison logic
      boolean arraysEqual = Arrays.deepEquals(sourceArray, (String[][][]) fullyPopulated);
      LOGGER.finer(() -> "Array comparison result: " + arraysEqual);
      assertTrue(arraysEqual);
    }
  }

  static int getArrayDimensions(TypeExpr arrayType) {
    if (arrayType instanceof TypeExpr.ArrayNode(var elementNode)) {
      if (elementNode instanceof TypeExpr.ArrayNode) {
        final var r = 1 + getArrayDimensions(((TypeExpr.ArrayNode) arrayType).element());
        LOGGER.finer(() -> "Array dimensions of " + arrayType.toTreeString() + "=" + r);
        return r;
      } else {
        throw new IllegalArgumentException("Unsupported element type: " + elementNode.getClass().getName());
      }
    }
    throw new IllegalArgumentException("Unsupported type: " + arrayType.getClass().getName());
  }

  static TypeExpr getArrayInnerType(TypeExpr arrayType) {
    if (!(arrayType instanceof TypeExpr.ArrayNode)) {
      return arrayType;
    }
    return getArrayInnerType(((TypeExpr.ArrayNode) arrayType).element());
  }

  static Object createArray(Class<?> componentType, int dimensions) {
    int[] dims = new int[dimensions];
    Arrays.fill(dims, 1); // Default size 1 for each dimension
    return Array.newInstance(componentType, dims);
  }

  static Object createAndPopulateArray(Object sourceArray, TypeExpr typeExpr, Class<?> javaType) {
    LOGGER.finer(() -> "Creating and populating array of type: " + typeExpr.toTreeString());

    int length = Array.getLength(sourceArray);
    LOGGER.finer(() -> "Source array length: " + length);

    Object newArray = Array.newInstance(
        typeExpr instanceof TypeExpr.ArrayNode ?
            javaType :
            Object.class,
        length
    );
    LOGGER.finer(() -> "Created new array of type: " + newArray.getClass().getTypeName());

    for (int i = 0; i < length; i++) {
      Object element = Array.get(sourceArray, i);
      int finalI = i;
      LOGGER.finer(() -> "Processing element " + finalI + ": " + element);

      assert typeExpr instanceof TypeExpr.ArrayNode;
      Object populatedElement = createAndPopulateArray(element, ((TypeExpr.ArrayNode) typeExpr).element(), javaType);
      int finalI1 = i;
      LOGGER.finer(() -> "Populated element " + finalI1 + ": " + populatedElement);

      Array.set(newArray, i, populatedElement);
    }
    LOGGER.finer(() -> "Array population complete");
    return newArray;
  }
}
