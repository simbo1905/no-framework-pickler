// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TypeExpr2ListTests {

  @Test
  void testListOfPrimitiveArrays() {
    record ListOfIntArrayHolder(List<int[]> value) {
    }
    var comp = ListOfIntArrayHolder.class.getRecordComponents()[0];
    var expr = TypeExpr2.analyzeType(comp.getGenericType(), java.util.List.of());
    assertThat(expr.toTreeString()).isEqualTo("LIST(ARRAY(int))");

    record ListOfDoubleArrayHolder(List<double[]> value) {
    }
    var comp2 = ListOfDoubleArrayHolder.class.getRecordComponents()[0];
    var expr2 = TypeExpr2.analyzeType(comp2.getGenericType(), java.util.List.of());
    assertThat(expr2.toTreeString()).isEqualTo("LIST(ARRAY(double))");
  }
}

