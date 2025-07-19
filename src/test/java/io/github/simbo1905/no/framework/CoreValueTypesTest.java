// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import io.github.simbo1905.LoggingControl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

public class CoreValueTypesTest {

  @BeforeAll
  static void setupLogging() {
    LoggingControl.setupCleanLogging();
  }

  @Test
  void testAllPrimitives() {
    final var record = new AllPrimitivesRecord(true, (byte) 1, (short) 2, 'c', 4, 5L, 6.0f, 7.0);
    final var pickler = Pickler.forClass(AllPrimitivesRecord.class);
    final var maxSizeOfRecord = pickler.maxSizeOf(record);
    final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
    final int bytesWritten = pickler.serialize(buffer, record);
    assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
    buffer.flip();
    final var deserialized = pickler.deserialize(buffer);
    assertThat(deserialized).isEqualTo(record);
  }

  @Test
  void testAllBoxed() {
    final var record = new AllBoxedRecord(true, (byte) 1, (short) 2, 'c', 4, 5L, 6.0f, 7.0);
    final var pickler = Pickler.forClass(AllBoxedRecord.class);
    final var maxSizeOfRecord = pickler.maxSizeOf(record);
    final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
    final int bytesWritten = pickler.serialize(buffer, record);
    assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
    buffer.flip();
    final var deserialized = pickler.deserialize(buffer);
    assertThat(deserialized).isEqualTo(record);
  }

  @Test
  void testBuiltInTypes() {
    final var record = new BuiltInTypesRecord("hello", LocalDate.now(), LocalDateTime.now());
    final var pickler = Pickler.forClass(BuiltInTypesRecord.class);
    final var maxSizeOfRecord = pickler.maxSizeOf(record);
    final ByteBuffer buffer = ByteBuffer.allocate(maxSizeOfRecord + 64); // Add some extra space for safety
    final int bytesWritten = pickler.serialize(buffer, record);
    assertThat(bytesWritten).isLessThanOrEqualTo(maxSizeOfRecord);
    buffer.flip();
    final var deserialized = pickler.deserialize(buffer);
    assertThat(deserialized).isEqualTo(record);
  }

  @Test
  void testNullableTypes() {
    final var pickler = Pickler.forClass(NullableTypesRecord.class);

    // Test with non-null values
    final var record1 = new NullableTypesRecord("not null", 42, 3.14);
    final var maxSizeOfRecord1 = pickler.maxSizeOf(record1);
    final ByteBuffer buffer1 = ByteBuffer.allocate(maxSizeOfRecord1 + 64);
    final int bytesWritten1 = pickler.serialize(buffer1, record1);
    assertThat(bytesWritten1).isLessThanOrEqualTo(maxSizeOfRecord1);
    buffer1.flip();
    final var deserialized1 = pickler.deserialize(buffer1);
    assertThat(deserialized1).isEqualTo(record1);

    // Test with all null values
    final var record2 = new NullableTypesRecord(null, null, null);
    final var maxSizeOfRecord2 = pickler.maxSizeOf(record2);
    final ByteBuffer buffer2 = ByteBuffer.allocate(maxSizeOfRecord2 + 64);
    final int bytesWritten2 = pickler.serialize(buffer2, record2);
    assertThat(bytesWritten2).isLessThanOrEqualTo(maxSizeOfRecord2);
    buffer2.flip();
    final var deserialized2 = pickler.deserialize(buffer2);
    assertThat(deserialized2).isEqualTo(record2);

    // Test with mixed null and non-null values
    final var record3 = new NullableTypesRecord("not null", null, 3.14);
    final var maxSizeOfRecord3 = pickler.maxSizeOf(record3);
    final ByteBuffer buffer3 = ByteBuffer.allocate(maxSizeOfRecord3 + 64);
    final int bytesWritten3 = pickler.serialize(buffer3, record3);
    assertThat(bytesWritten3).isLessThanOrEqualTo(maxSizeOfRecord3);
    buffer3.flip();
    final var deserialized3 = pickler.deserialize(buffer3);
    assertThat(deserialized3).isEqualTo(record3);
  }

  // 1. CoreValueTypesTest
  // Purpose: Test each primitive, boxed, and built-in value type once
  public record AllPrimitivesRecord(boolean b, byte by, short s, char c, int i, long l, float f, double d) {
  }

  public record AllBoxedRecord(Boolean b, Byte by, Short s, Character c, Integer i, Long l, Float f, Double d) {
  }

  public record BuiltInTypesRecord(String str, LocalDate date, LocalDateTime dateTime) {
  }

  public record NullableTypesRecord(String str, Integer i, Double d) {
  }
}
