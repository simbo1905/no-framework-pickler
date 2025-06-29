// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

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

  @Test
  @DisplayName("3D String array dimensions level check")
  void test3DStringArrayDimensionInner() throws NoSuchFieldException {
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
  }


  @Test
  @DisplayName("3D String array dimensions")
  void test3DStringArrayDimensions() throws NoSuchFieldException {
    Type arrayType = NestedArraysTypeExprTest.class.getDeclaredField("aStringArrayThree").getGenericType();
    LOGGER.finer(() -> "Analyzing array type: " + arrayType.getTypeName());
    TypeExpr actual = TypeExpr.analyze(arrayType);
    LOGGER.finer(() -> "Actual TypeExpr: " + actual.toTreeString());
    final int dimensionsActual = getArrayDimensions(actual);
    LOGGER.finer(() -> "Array dimensions: " + dimensionsActual);
    assertThat(dimensionsActual).isEqualTo(3);

    final TypeExpr innerReferenceType = getArrayInnerType(actual);
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


  static int getArrayDimensions(TypeExpr arrayType) {
    return arrayDimensionsInner(arrayType, 0);
  }

  static int arrayDimensionsInner(TypeExpr type, int currentCount) {
    if (type instanceof TypeExpr.ArrayNode) {
      // We are at an array node, so we add one and then look at the element type.
      return arrayDimensionsInner(((TypeExpr.ArrayNode) type).element(), currentCount + 1);
    } else {
      // We've hit a non-array node. The currentCount is the total number of array dimensions we have traversed.
      return currentCount;
    }
  }

  static TypeExpr getArrayInnerType(TypeExpr arrayType) {
    if (!(arrayType instanceof TypeExpr.ArrayNode)) {
      return arrayType;
    }
    return getArrayInnerType(((TypeExpr.ArrayNode) arrayType).element());
  }

  @SuppressWarnings("SameParameterValue")
  static Object createArray(Class<?> componentType, int dimensions) {
    int[] dims = new int[dimensions];
    Arrays.fill(dims, 1); // Default size 1 for each dimension
    return Array.newInstance(componentType, dims);
  }

  static Class<?> typeExprToClass(TypeExpr typeExpr) {
    return switch (typeExpr) {
      case TypeExpr.ArrayNode(var element) -> {
        Class<?> componentClass = typeExprToClass(element);
        yield Array.newInstance(componentClass, 0).getClass();
      }
      case TypeExpr.RefValueNode(var type, var javaType) -> (Class<?>) javaType;
      case TypeExpr.PrimitiveValueNode(var type, var javaType) -> (Class<?>) javaType;
      default -> throw new IllegalArgumentException("Unsupported TypeExpr: " + typeExpr);
    };
  }


  static Object createAndPopulateArray(Object sourceArray, TypeExpr typeExpr, Class<?> javaType) {
    LOGGER.finer(() -> "createAndPopulateArray called with sourceArray: "
        + (sourceArray == null ? "null" : sourceArray.getClass().getTypeName())
        + ", typeExpr: " + typeExpr.toTreeString()
        + ", javaType: " + javaType.getTypeName());

    // If the current type expression is not an array, then we are at a leaf -> return the sourceArray as is.
    if (!(typeExpr instanceof TypeExpr.ArrayNode arrayNode)) {
      LOGGER.fine(() -> "Leaf node reached, returning sourceArray: " + sourceArray);
      return sourceArray;
    }
    int length = Array.getLength(sourceArray);
    LOGGER.finer(() -> "Source array length: " + length);

    // Get the correct component type from the TypeExpr structure
    Class<?> componentType = typeExprToClass(arrayNode.element());
    Object newArray = Array.newInstance(componentType, length);

    LOGGER.finer(() -> "Creating array for: "
        + typeExpr.toTreeString()
        + " | Component type: " + componentType.getName()
        + " | Source component: " + sourceArray.getClass().getComponentType().getName()
        + " | New array class: " + newArray.getClass().getName()
    );

    for (int i = 0; i < length; i++) {
      Object element = Array.get(sourceArray, i);
      int finalI = i;
      LOGGER.finer(() -> "Processing element " + finalI + ": " + element);

      Object populatedElement = createAndPopulateArray(element, arrayNode.element(), javaType);

      int finalI1 = i;
      LOGGER.finer(() -> "Populated element " + finalI1 + ": " + populatedElement);
      Array.set(newArray, i, populatedElement);
    }

    LOGGER.finer(() -> "Array population complete");
    return newArray;
  }
}
