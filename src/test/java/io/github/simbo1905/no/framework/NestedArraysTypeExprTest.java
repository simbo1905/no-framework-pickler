// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Type;
import java.util.Arrays;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NestedArraysTypeExprTest {

  @BeforeAll
  static void setupLogging() {
    io.github.simbo1905.LoggingControl.setupCleanLogging();
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

    final int dimensionsActual = Companion.getArrayDimensions(actual);
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
    final int dimensionsActual = Companion.getArrayDimensions(actual);
    LOGGER.finer(() -> "Array dimensions: " + dimensionsActual);
    assertThat(dimensionsActual).isEqualTo(3);

    final TypeExpr innerReferenceType = Companion.getArrayInnerType(actual);
    assertThat(innerReferenceType).isEqualTo(
        new TypeExpr.RefValueNode(TypeExpr.RefValueType.STRING, String.class)
    );

    final Object reflectivelyCreated = Companion.createArray(String.class, 3);
    LOGGER.finer(() -> "Reflectively created array type: " + reflectivelyCreated.getClass().getTypeName());
    assertThat(reflectivelyCreated.getClass()).isEqualTo(String[][][].class);

    final var sourceArray = NestedArraysTypeExprTest.this.aStringArrayThree;
    LOGGER.finer(() -> "Source array structure: " + Arrays.deepToString(sourceArray));

    final Object fullyPopulated = Companion.createAndPopulateArray(sourceArray, actual, String.class);
    LOGGER.finer(() -> "Fully populated array type: " + fullyPopulated.getClass().getTypeName());

    // Implement array comparison logic
    boolean arraysEqual = Arrays.deepEquals(sourceArray, (String[][][]) fullyPopulated);
    LOGGER.finer(() -> "Array comparison result: " + arraysEqual);
    assertTrue(arraysEqual);
  }

  @Test
  @DisplayName("1D String array dimensions level check")
  void test1DStringArrayDimensionInner() throws NoSuchFieldException {
    LOGGER.info(() -> "Starting test for 1D String array dimensions");
    Type arrayType = NestedArraysTypeExprTest.class.getDeclaredField("aStringArrayOne").getGenericType();
    LOGGER.finer(() -> "Analyzing array type: " + arrayType.getTypeName());
    TypeExpr expected = new TypeExpr.ArrayNode(
        new TypeExpr.RefValueNode(TypeExpr.RefValueType.STRING, String.class)
    );
    LOGGER.finer(() -> "Expected TypeExpr: " + expected.toTreeString());
    TypeExpr actual = TypeExpr.analyze(arrayType);
    LOGGER.finer(() -> "Actual TypeExpr: " + actual.toTreeString());
    assertEquals(expected, actual);
    assertThat(actual.toTreeString()).isEqualTo("ARRAY(String)");
    final int dimensionsActual = Companion.getArrayDimensions(actual);
    LOGGER.finer(() -> "Array dimensions: " + dimensionsActual);
    assertThat(dimensionsActual).isEqualTo(1);
  }

  @Test
  @DisplayName("2D String array dimensions level check")
  void test2DStringArrayDimensionInner() throws NoSuchFieldException {
    LOGGER.info(() -> "Starting test for 2D String array dimensions");
    Type arrayType = NestedArraysTypeExprTest.class.getDeclaredField("aStringArrayTwo").getGenericType();
    LOGGER.finer(() -> "Analyzing array type: " + arrayType.getTypeName());
    TypeExpr expected = new TypeExpr.ArrayNode(
        new TypeExpr.ArrayNode(
            new TypeExpr.RefValueNode(TypeExpr.RefValueType.STRING, String.class)
        )
    );
    LOGGER.finer(() -> "Expected TypeExpr: " + expected.toTreeString());
    TypeExpr actual = TypeExpr.analyze(arrayType);
    LOGGER.finer(() -> "Actual TypeExpr: " + actual.toTreeString());
    assertEquals(expected, actual);
    assertThat(actual.toTreeString()).isEqualTo("ARRAY(ARRAY(String))");
    final int dimensionsActual = Companion.getArrayDimensions(actual);
    LOGGER.finer(() -> "Array dimensions: " + dimensionsActual);
    assertThat(dimensionsActual).isEqualTo(2);
  }

  @Test
  @DisplayName("1D Primitive Int array dimensions level check")
  void test1DPrimitiveIntArrayDimensionInner() throws NoSuchFieldException {
    LOGGER.info(() -> "Starting test for 1D Primitive Int array dimensions");
    Type arrayType = NestedArraysTypeExprTest.class.getDeclaredField("anIntArrayOne").getGenericType();
    LOGGER.finer(() -> "Analyzing array type: " + arrayType.getTypeName());
    TypeExpr expected = new TypeExpr.ArrayNode(
        new TypeExpr.PrimitiveValueNode(TypeExpr.PrimitiveValueType.INTEGER, int.class)
    );
    LOGGER.finer(() -> "Expected TypeExpr: " + expected.toTreeString());
    TypeExpr actual = TypeExpr.analyze(arrayType);
    LOGGER.finer(() -> "Actual TypeExpr: " + actual.toTreeString());
    assertEquals(expected, actual);
    assertThat(actual.toTreeString()).isEqualTo("ARRAY(int)");
    final int dimensionsActual = Companion.getArrayDimensions(actual);
    LOGGER.finer(() -> "Array dimensions: " + dimensionsActual);
    assertThat(dimensionsActual).isEqualTo(1);
  }

  @Test
  @DisplayName("2D Primitive Int array dimensions level check")
  void test2DPrimitiveIntArrayDimensionInner() throws NoSuchFieldException {
    LOGGER.info(() -> "Starting test for 2D Primitive Int array dimensions");
    Type arrayType = NestedArraysTypeExprTest.class.getDeclaredField("anIntArrayTwo").getGenericType();
    LOGGER.finer(() -> "Analyzing array type: " + arrayType.getTypeName());
    TypeExpr expected = new TypeExpr.ArrayNode(
        new TypeExpr.ArrayNode(
            new TypeExpr.PrimitiveValueNode(TypeExpr.PrimitiveValueType.INTEGER, int.class)
        )
    );
    LOGGER.finer(() -> "Expected TypeExpr: " + expected.toTreeString());
    TypeExpr actual = TypeExpr.analyze(arrayType);
    LOGGER.finer(() -> "Actual TypeExpr: " + actual.toTreeString());
    assertEquals(expected, actual);
    assertThat(actual.toTreeString()).isEqualTo("ARRAY(ARRAY(int))");
    final int dimensionsActual = Companion.getArrayDimensions(actual);
    LOGGER.finer(() -> "Array dimensions: " + dimensionsActual);
    assertThat(dimensionsActual).isEqualTo(2);
  }

  @Test
  @DisplayName("3D Primitive Int array dimensions level check")
  void test3DPrimitiveIntArrayDimensionInner() throws NoSuchFieldException {
    LOGGER.info(() -> "Starting test for 3D Primitive Int array dimensions");
    Type arrayType = NestedArraysTypeExprTest.class.getDeclaredField("anIntArrayThree").getGenericType();
    LOGGER.finer(() -> "Analyzing array type: " + arrayType.getTypeName());
    TypeExpr expected = new TypeExpr.ArrayNode(
        new TypeExpr.ArrayNode(
            new TypeExpr.ArrayNode(
                new TypeExpr.PrimitiveValueNode(TypeExpr.PrimitiveValueType.INTEGER, int.class)
            )
        )
    );
    LOGGER.finer(() -> "Expected TypeExpr: " + expected.toTreeString());
    TypeExpr actual = TypeExpr.analyze(arrayType);
    LOGGER.finer(() -> "Actual TypeExpr: " + actual.toTreeString());
    assertEquals(expected, actual);
    assertThat(actual.toTreeString()).isEqualTo("ARRAY(ARRAY(ARRAY(int)))");
    final int dimensionsActual = Companion.getArrayDimensions(actual);
    LOGGER.finer(() -> "Array dimensions: " + dimensionsActual);
    assertThat(dimensionsActual).isEqualTo(3);
  }

}
