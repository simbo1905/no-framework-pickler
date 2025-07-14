// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TypeExpr2PrimitiveArrayTests {

  public record AllPrimitiveArrays(
      boolean[] booleans,
      byte[] bytes,
      short[] shorts,
      char[] chars,
      int[] ints,
      long[] longs,
      float[] floats,
      double[] doubles
  ) {
  }

  @Test
  void testAllPrimitiveArraysRoundTrip() {
    var original = new AllPrimitiveArrays(
        new boolean[]{true, false, true},
        new byte[]{1, 2, 3},
        new short[]{4, 5, 6},
        new char[]{'a', 'b', 'c'},
        new int[]{10, 20, 300},
        new long[]{100L, 200L, 3000L},
        new float[]{1.1f, 2.2f, 3.3f},
        new double[]{10.1, 20.2, 30.3}
    );

    ComponentSerde[] serdes = Companion2.buildComponentSerdes(
        AllPrimitiveArrays.class,
        List.of(),
        type -> obj -> 1024, // Simplified sizer
        type -> (buffer, obj) -> {
        }, // Simplified writer
        signature -> buffer -> null // Simplified reader
    );

    ByteBuffer buffer = ByteBuffer.allocate(1024);

    for (ComponentSerde serde : serdes) {
      serde.writer().accept(buffer, original);
    }

    buffer.flip();

    boolean[] readBooleans = (boolean[]) serdes[0].reader().apply(buffer);
    byte[] readBytes = (byte[]) serdes[1].reader().apply(buffer);
    short[] readShorts = (short[]) serdes[2].reader().apply(buffer);
    char[] readChars = (char[]) serdes[3].reader().apply(buffer);
    int[] readInts = (int[]) serdes[4].reader().apply(buffer);
    long[] readLongs = (long[]) serdes[5].reader().apply(buffer);
    float[] readFloats = (float[]) serdes[6].reader().apply(buffer);
    double[] readDoubles = (double[]) serdes[7].reader().apply(buffer);

    var deserialized = new AllPrimitiveArrays(
        readBooleans,
        readBytes,
        readShorts,
        readChars,
        readInts,
        readLongs,
        readFloats,
        readDoubles
    );

    assertThat(deserialized.booleans).isEqualTo(original.booleans);
    assertThat(deserialized.bytes).isEqualTo(original.bytes);
    assertThat(deserialized.shorts).isEqualTo(original.shorts);
    assertThat(deserialized.chars).isEqualTo(original.chars);
    assertThat(deserialized.ints).isEqualTo(original.ints);
    assertThat(deserialized.longs).isEqualTo(original.longs);
    assertThat(deserialized.floats).isEqualTo(original.floats);
    assertThat(deserialized.doubles).isEqualTo(original.doubles);
  }
}

