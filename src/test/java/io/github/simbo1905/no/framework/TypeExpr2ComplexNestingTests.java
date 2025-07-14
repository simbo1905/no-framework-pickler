// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class TypeExpr2ComplexNestingTests {

  @Test
  void testAnalysisOfHighlyNestedType() {
    record ExtremeNesting(List<Map<String, Optional<Integer[]>[][]>> value) {
    }
    var components = ExtremeNesting.class.getRecordComponents();
    var expr = TypeExpr2.analyzeType(components[0].getGenericType(), List.of());
    assertThat(expr.toTreeString()).isEqualTo("LIST(MAP(String,ARRAY(ARRAY(OPTIONAL(ARRAY(Integer))))))");
  }
}

