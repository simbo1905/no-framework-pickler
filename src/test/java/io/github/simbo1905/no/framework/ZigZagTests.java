// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ZigZagTests {

  @Test
  void testMaxLongValue() {
    long maxValue = Long.MAX_VALUE; // 0x7FFFFFFFFFFFFFFF
    ByteBuffer buffer = ByteBuffer.allocate(9);

    ZigZagEncoding.putLong(buffer, maxValue);
    buffer.flip();

    long result = ZigZagEncoding.getLong(buffer);
    assertEquals(maxValue, result);
  }

  @Test
  void testCurrentTimeAsLong() {
    long timeInMs = System.currentTimeMillis();
    ByteBuffer buffer = ByteBuffer.allocate(9);

    ZigZagEncoding.putLong(buffer, timeInMs);
    buffer.flip();

    long result = ZigZagEncoding.getLong(buffer);
    assertEquals(timeInMs, result);
  }

  @Test
  void testZero() {
    long value = 0L;
    ByteBuffer buffer = ByteBuffer.allocate(1);

    ZigZagEncoding.putLong(buffer, value);
    buffer.flip();

    long result = ZigZagEncoding.getLong(buffer);
    assertEquals(value, result);
  }

  @Test
  void testPositiveNumbers() {
    long value = 1234567890L;
    ByteBuffer buffer = ByteBuffer.allocate(9);

    ZigZagEncoding.putLong(buffer, value);
    buffer.flip();

    long result = ZigZagEncoding.getLong(buffer);
    assertEquals(value, result);
  }

  @Test
  void testNegativeNumbers() {
    long value = -1234567890L;
    ByteBuffer buffer = ByteBuffer.allocate(9);

    ZigZagEncoding.putLong(buffer, value);
    buffer.flip();

    long result = ZigZagEncoding.getLong(buffer);
    assertEquals(value, result);
  }

  @Test
  void testMinLongValue() {
    long minValue = Long.MIN_VALUE; // 0x8000000000000000
    ByteBuffer buffer = ByteBuffer.allocate(9);

    ZigZagEncoding.putLong(buffer, minValue);
    buffer.flip();

    long result = ZigZagEncoding.getLong(buffer);
    assertEquals(minValue, result);
  }

  @Test
  void testIntValues() {
    int value = 12345;
    ByteBuffer buffer = ByteBuffer.allocate(5);

    ZigZagEncoding.putInt(buffer, value);
    buffer.flip();

    int result = ZigZagEncoding.getInt(buffer);
    assertEquals(value, result);
  }

  @Test
  void testIntMax() {
    ByteBuffer buffer = ByteBuffer.allocate(5);

    ZigZagEncoding.putInt(buffer, Integer.MAX_VALUE);
    buffer.flip();

    int result = ZigZagEncoding.getInt(buffer);
    assertEquals(Integer.MAX_VALUE, result);
  }

  @Test
  void testNegativeIntValues() {
    int value = -12345;
    ByteBuffer buffer = ByteBuffer.allocate(5);

    ZigZagEncoding.putInt(buffer, value);
    buffer.flip();

    int result = ZigZagEncoding.getInt(buffer);
    assertEquals(value, result);
  }

  @Test
  void testSizeOfInt() {
    assertEquals(1, ZigZagEncoding.sizeOf(0));
    assertEquals(1, ZigZagEncoding.sizeOf(1));
    assertEquals(1, ZigZagEncoding.sizeOf(-1));
    assertEquals(1, ZigZagEncoding.sizeOf(63));
    assertEquals(1, ZigZagEncoding.sizeOf(-64));
    assertEquals(2, ZigZagEncoding.sizeOf(64));
    assertEquals(2, ZigZagEncoding.sizeOf(-65));
    assertEquals(3, ZigZagEncoding.sizeOf(16384));
    assertEquals(3, ZigZagEncoding.sizeOf(-16385));
    assertEquals(4, ZigZagEncoding.sizeOf(8388608));
    assertEquals(4, ZigZagEncoding.sizeOf(-8388609));
    assertEquals(5, ZigZagEncoding.sizeOf(Integer.MAX_VALUE));
    assertEquals(5, ZigZagEncoding.sizeOf(Integer.MIN_VALUE));
  }

  @Test
  void testSizeOfLong() {
    assertEquals(1, ZigZagEncoding.sizeOf(0L));
    assertEquals(1, ZigZagEncoding.sizeOf(1L));
    assertEquals(1, ZigZagEncoding.sizeOf(-1L));
    assertEquals(1, ZigZagEncoding.sizeOf(63L));
    assertEquals(1, ZigZagEncoding.sizeOf(-64L));
    assertEquals(2, ZigZagEncoding.sizeOf(64L));
    assertEquals(2, ZigZagEncoding.sizeOf(-65L));
    assertEquals(9, ZigZagEncoding.sizeOf(Long.MAX_VALUE));
    assertEquals(9, ZigZagEncoding.sizeOf(Long.MIN_VALUE));
  }

  @Test
  void testSizeOfMatchesPut() {
    ByteBuffer buffer = ByteBuffer.allocate(10); // Max 9 for long + 1 extra
    long[] testValues = {
        0L, 1L, -1L, 63L, -64L, 64L, -65L,
        12345L, -12345L,
        Integer.MAX_VALUE, Integer.MIN_VALUE,
        (long) Integer.MAX_VALUE + 1, (long) Integer.MIN_VALUE - 1,
        Long.MAX_VALUE, Long.MIN_VALUE
    };

    for (long value : testValues) {
      buffer.clear();
      ZigZagEncoding.putLong(buffer, value);
      int bytesWritten = buffer.position();
      assertEquals(ZigZagEncoding.sizeOf(value), bytesWritten, "Size mismatch for long value: " + value);

      if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
        int intValue = (int) value;
        buffer.clear();
        ZigZagEncoding.putInt(buffer, intValue);
        bytesWritten = buffer.position();
        assertEquals(ZigZagEncoding.sizeOf(intValue), bytesWritten, "Size mismatch for int value: " + intValue);
      }
    }
  }

}
