// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import io.github.simbo1905.LoggingControl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/// Package-private tests for core machinery components
/// Tests the internal implementation details that are not part of the public API
public class RefValueTests {

  @BeforeAll
  static void setupLogging() {
    LoggingControl.setupCleanLogging();
  }

  public static RefValueRecord pefValueRecordNotNull =
      new RefValueRecord(true, (byte) 1, 'a', (short) 2, 3, 4L, 5.0f, 6.0);

  public static RefValueRecord refValueRecordNull =
      new RefValueRecord(null, null, null, null, null, null, null, null);


  @BeforeEach
  void setUp() {
    LOGGER.fine(() -> "Starting RefValueTests test");
  }

  @AfterEach
  void tearDown() {
    LOGGER.fine(() -> "Finished RefValueTests test");
  }

  @Test
  @DisplayName("Test references not null")
  void testReferencesNotNull() {
    final var pickler = Pickler.forClass(RefValueRecord.class);
    final var buffer = ByteBuffer.allocate(pickler.maxSizeOf(pefValueRecordNotNull));
    pickler.serialize(buffer, pefValueRecordNotNull);
    buffer.flip(); // Prepare for reading
    final var deserialized = pickler.deserialize(buffer);
    assertThat(deserialized).isEqualTo(pefValueRecordNotNull);

  }

  @Test
  @DisplayName("Test references are null")
  void testReferencesAreNull() {
    final var pickler = Pickler.forClass(RefValueRecord.class);
    final var buffer = ByteBuffer.allocate(pickler.maxSizeOf(refValueRecordNull));
    pickler.serialize(buffer, refValueRecordNull);
    buffer.flip(); // Prepare for reading
    final var deserialized = pickler.deserialize(buffer);
    assertThat(deserialized).isEqualTo(refValueRecordNull);

  }

  public record RefValueRecord(
      Boolean booleanValue,
      Byte byteValue,
      Character charValue,
      Short shortValue,
      Integer intValue,
      Long longValue,
      Float floatValue,
      Double doubleValue
  ) {
  }

  public record Record1DPrimitiveArray(int[] ints) {
  }

  public record Record1DRefArray(String[] strings) {
  }

  @Test
  @DisplayName("Test 1D primitive array serialization")
  void test1DArrayPrimitive() {
    final var original = new Record1DPrimitiveArray(new int[]{1, 2, 3});
    final var pickler = Pickler.forClass(Record1DPrimitiveArray.class);
    final var buffer = ByteBuffer.allocate(pickler.maxSizeOf(original));
    pickler.serialize(buffer, original);
    buffer.flip(); // Prepare for reading
    final var deserialized = pickler.deserialize(buffer);
    assertArrayEquals(original.ints(), deserialized.ints());
  }

  @Test
  @DisplayName("Test 1D ref array serialization")
  void test1DArrayRef() {
    final var original = new Record1DRefArray(new String[]{"a", "b", "c"});
    final var pickler = Pickler.forClass(Record1DRefArray.class);
    final var buffer = ByteBuffer.allocate(pickler.maxSizeOf(original));
    pickler.serialize(buffer, original);
    buffer.flip(); // Prepare for reading
    final var deserialized = pickler.deserialize(buffer);
    assertArrayEquals(original.strings(), deserialized.strings());
  }

  public record Record2DPrimitiveArray(int[][] ints) {
  }

  public record Record2DRefArray(String[][] strings) {
  }


  @Test
  @DisplayName("Test 2D primitive array")
  void test2DArrayPrimitive() {
    LOGGER.fine(() -> "---------------\nTesting 2D primitive array test2DArrayPrimitive");
    final var original = new Record2DPrimitiveArray(new int[][]{{1, 2}, {3, 4}});
    final var pickler = Pickler.forClass(Record2DPrimitiveArray.class);
    final var buffer = ByteBuffer.allocate(pickler.maxSizeOf(original));
    pickler.serialize(buffer, original);
    buffer.flip(); // Prepare for reading
    final var deserialized = pickler.deserialize(buffer);
    // perform deep arrays equals checks
    final var expected = original.ints();
    final var actual = deserialized.ints();
    assertArrayEquals(expected, actual, "Deserialized 2D primitive array should equal the original");
  }


  @Test
  @DisplayName("Test 2D ref array ")
  void test2DArrayRef() {
    LOGGER.fine(() -> "---------------\nTesting 2D ref array test2DArrayRef");
    final var original = new Record2DRefArray(new String[][]{{"a", "b"}, {"c", "d"}});
    final var pickler = Pickler.forClass(Record2DRefArray.class);
    final var buffer = ByteBuffer.allocate(pickler.maxSizeOf(original));
    pickler.serialize(buffer, original);
    buffer.flip(); // Prepare for reading
    final var deserialized = pickler.deserialize(buffer);
    // perform deep arrays equals checks
    final var expected = original.strings();
    final var actual = deserialized.strings();
    assertArrayEquals(expected, actual, "Deserialized 2D ref array should equal the original");
  }
}
